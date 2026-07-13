package memstore

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
)

func TestBatchStore_OpenAndTransition(t *testing.T) {
	ctx := context.Background()
	s := NewBatchStore()
	b, err := s.OpenBatch(ctx, "BTC/USD")
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != store.BatchOpen {
		t.Fatalf("expected open, got %s", b.Status)
	}
	// Idempotent: open returns the same open batch.
	b2, _ := s.OpenBatch(ctx, "BTC/USD")
	if b2.ID != b.ID {
		t.Fatalf("expected same batch id, got %d vs %d", b2.ID, b.ID)
	}
	// Transition open -> closed.
	updated, ok, err := s.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, func(x *store.Batch) { x.NotionalUSD = 100 })
	if err != nil || !ok {
		t.Fatalf("transition open->closed: %v ok=%v", err, ok)
	}
	if updated.Status != store.BatchClosed {
		t.Fatalf("expected closed, got %s", updated.Status)
	}
	if updated.NotionalUSD != 100 {
		t.Fatalf("expected notional 100, got %f", updated.NotionalUSD)
	}
	if updated.ClosedAt.IsZero() {
		t.Fatal("expected closed_at set")
	}
	// Invalid transition closed -> settled (wrong from-status returns no-op).
	_, ok2, err := s.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchSettled, nil)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if ok2 {
		t.Fatal("expected no-op on from mismatch")
	}
}

func TestMembershipStore_DedupByTxID(t *testing.T) {
	ctx := context.Background()
	bs := NewBatchStore()
	b, _ := bs.OpenBatch(ctx, "ETH/USD")
	ms := NewMembershipStore()
	m := &store.Membership{BatchID: b.ID, TxID: "tx1", Amount: 1, Asset: "ETH", FiatCurrency: "USD", NotionalUSD: 2000}
	ok, err := ms.AddMembership(ctx, m)
	if err != nil || !ok {
		t.Fatalf("first add: %v ok=%v", err, ok)
	}
	ok, err = ms.AddMembership(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected dup add to return false")
	}
	exists, _ := ms.ExistsByTxID(ctx, "tx1")
	if !exists {
		t.Fatal("expected exists")
	}
	sum, _ := ms.SumNotional(ctx, b.ID)
	if sum != 2000 {
		t.Fatalf("sum=%f want 2000", sum)
	}
}

func TestAggregateOrderStore_CreateIdempotent(t *testing.T) {
	ctx := context.Background()
	s := NewAggregateOrderStore()
	o := &store.AggregateOrder{BatchID: 1, AssetPair: "BTC/USD", NotionalUSD: 50000}
	a, err := s.CreateOrder(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != store.AggregateExecuting {
		t.Fatalf("expected executing, got %s", a.Status)
	}
	b, _ := s.CreateOrder(ctx, o)
	if b.ID != a.ID {
		t.Fatalf("expected idempotent create, got %d vs %d", b.ID, a.ID)
	}
	updated, _ := s.UpdateOrderFill(ctx, 1, 50001, 1, []store.VenueRoute{{Venue: "v1", Share: 1, Price: 50001}})
	if updated.FillPrice != 50001 {
		t.Fatalf("fill_price=%f want 50001", updated.FillPrice)
	}
	settled, _ := s.SettleOrder(ctx, 1, 40000)
	if settled.Status != store.AggregateSettled {
		t.Fatalf("expected settled, got %s", settled.Status)
	}
	if settled.HedgedNotional != 40000 {
		t.Fatalf("hedged=%f want 40000", settled.HedgedNotional)
	}
}

func TestFloatStore_AddAndSettle(t *testing.T) {
	ctx := context.Background()
	s := NewFloatStore()
	due := time.Now().UTC().Add(48 * time.Hour)
	p, err := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: 1000, LongCryptoAmount: 0.02, LongCryptoAsset: "BTC", SettlementDueAt: due})
	if err != nil {
		t.Fatal(err)
	}
	if p.ShortFiatAmount != 1000 {
		t.Fatalf("short=%f want 1000", p.ShortFiatAmount)
	}
	// Accumulate into the same row.
	p2, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: 500, LongCryptoAmount: 0.01, LongCryptoAsset: "BTC"})
	if p2.ID != p.ID {
		t.Fatalf("expected same row, got %d vs %d", p2.ID, p.ID)
	}
	if p2.ShortFiatAmount != 1500 {
		t.Fatalf("short=%f want 1500", p2.ShortFiatAmount)
	}
	// GetFloat aggregates.
	agg, _ := s.GetFloat(ctx, "USD")
	if agg.ShortFiatAmount != 1500 {
		t.Fatalf("agg short=%f want 1500", agg.ShortFiatAmount)
	}
	// Matured list (due in the past).
	before := time.Now().UTC()
	past, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "EUR", ShortFiatAmount: 100, LongCryptoAmount: 0.001, LongCryptoAsset: "BTC", SettlementDueAt: before.Add(-1 * time.Hour)})
	matured, _ := s.ListMaturedFloat(ctx, before.Add(time.Minute))
	if len(matured) != 1 || matured[0].ID != past.ID {
		t.Fatalf("expected 1 matured, got %v", matured)
	}
	if _, err := s.SettleFloat(ctx, past.ID); err != nil {
		t.Fatal(err)
	}
	matured2, _ := s.ListMaturedFloat(ctx, time.Now().Add(time.Hour))
	if len(matured2) != 0 {
		t.Fatalf("expected 0 matured after settle, got %d", len(matured2))
	}
}

func TestOutboxStore_Dedup(t *testing.T) {
	ctx := context.Background()
	s := NewOutboxStore()
	e := &store.OutboxEntry{Aggregate: "batch", EventType: "batch.close", DedupKey: "batch.close:1", Payload: []byte("{}")}
	ok, _ := s.Append(ctx, e)
	if !ok {
		t.Fatal("expected first append ok")
	}
	ok, _ = s.Append(ctx, e)
	if ok {
		t.Fatal("expected dup append false")
	}
	pending, _ := s.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}
	if err := s.MarkEmitted(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	pending2, _ := s.ListPending(ctx, 10)
	if len(pending2) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending2))
	}
}