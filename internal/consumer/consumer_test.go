package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/eventbus"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func newDeps(t *testing.T) (*Consumer, *memstore.All, *eventbus.InMemorySubscriber, idempotency.Store) {
	t.Helper()
	all := memstore.NewAll()
	bus := eventbus.NewInMemory()
	idem := idempotency.NewMem()
	c := New(Deps{
		Topic:       "tx.completed",
		Batches:     all.Batch,
		Memberships: all.Membership,
		Idem:        idem,
		Subscriber:  bus,
	})
	return c, all, bus, idem
}

func TestConsumer_AutoOpensBatchAndDedupes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, all, bus, _ := newDeps(t)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	ev := eventbus.TxCompletedEvent{TxID: "tx1", Amount: 1, Asset: "BTC", FiatCurrency: "USD", NotionalUSD: 1000}
	_ = bus.Push(ctx, "tx.completed", ev)
	_ = bus.Push(ctx, "tx.completed", ev) // dup

	// Wait for processing.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		exists, _ := all.Membership.ExistsByTxID(ctx, "tx1")
		if exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	exists, _ := all.Membership.ExistsByTxID(ctx, "tx1")
	if !exists {
		t.Fatal("expected membership")
	}
	open, _ := all.Batch.ListOpenBatches(ctx)
	if len(open) != 1 {
		t.Fatalf("expected 1 open batch, got %d", len(open))
	}
	if open[0].AssetPair != "BTC/USD" {
		t.Fatalf("pair=%s want BTC/USD", open[0].AssetPair)
	}
	ms, _ := all.Membership.ListMemberships(ctx, open[0].ID)
	if len(ms) != 1 {
		t.Fatalf("expected 1 membership after dup, got %d", len(ms))
	}
}

func TestConsumer_DeadLettersPoisonMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _, bus, _ := newDeps(t)
	dl := make(chan DeadLetter, 8)
	c.Deps.DeadLetters = dl
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "", Asset: "BTC"})
	select {
	case got := <-dl:
		if got.Reason == "" {
			t.Fatal("expected reason")
		}
	case <-time.After(time.Second):
		t.Fatal("expected dead-letter")
	}
}

func TestConsumer_OpenBatchReusedPerAssetPair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, all, bus, _ := newDeps(t)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond) // allow subscriber to register

	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "a", Asset: "BTC", FiatCurrency: "USD"})
	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "b", Asset: "BTC", FiatCurrency: "USD"})
	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "c", Asset: "ETH", FiatCurrency: "USD"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		open, _ := all.Batch.ListOpenBatches(ctx)
		if len(open) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	open, _ := all.Batch.ListOpenBatches(ctx)
	if len(open) != 2 {
		t.Fatalf("expected 2 open batches (BTC/USD, ETH/USD), got %d", len(open))
	}
}