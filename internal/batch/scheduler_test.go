package batch

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func newSchedulerDeps(t *testing.T, cfg config.Config) (*Scheduler, *memstore.All, *idempotency.MemStore) {
	t.Helper()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	lock := idempotency.NewCadenceLock(idem, time.Duration(cfg.BatchIntervalSeconds)*time.Second)
	var closed []*store.Batch
	s := New(Deps{
		Cfg:         cfg,
		Batches:     all.Batch,
		Memberships: all.Membership,
		Lock:        lock,
		OnClose: func(ctx context.Context, b *store.Batch, reason CloseReason) {
			closed = append(closed, b)
		},
	})
	_ = closed
	return s, all, idem
}

func TestScheduler_SizeThresholdClosesBatch(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: 5000}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", NotionalUSD: 6000, Asset: "BTC", FiatCurrency: "USD"})
	_ = all.Batch.SetBatchNotional(ctx, b.ID, 6000)
	reason := s.shouldClose(ctx, b)
	if reason != ReasonSize {
		t.Fatalf("reason=%q want size", reason)
	}
	closed, err := s.CloseBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", closed.Status)
	}
}

func TestScheduler_ManualCloseForcesAggregation(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: 100000}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// No memberships; thresholds unmet.
	reason := s.shouldClose(ctx, b)
	if reason != "" {
		t.Fatalf("reason=%q want empty", reason)
	}
	closed, err := s.CloseBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", closed.Status)
	}
	// Closing again fails.
	if _, err := s.CloseBatch(ctx, b.ID); err != ErrNotOpen {
		t.Fatalf("expected ErrNotOpen, got %v", err)
	}
}

func TestScheduler_TimeThresholdClosesBatch(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: 1000000}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Rely on the small interval (1s) and wait so the time threshold
	// triggers.
	_ = b
	time.Sleep(1100 * time.Millisecond)
	reason := s.shouldClose(ctx, b)
	if reason != ReasonTime {
		t.Fatalf("reason=%q want time", reason)
	}
}

func TestScheduler_TickClosesOpenBatches(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: 1000000}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Make it old enough by waiting.
	time.Sleep(1100 * time.Millisecond)
	s.tick(ctx)
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", got.Status)
	}
}

func TestScheduler_CadenceLockPreventsDoubleClose(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: 1000000}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	time.Sleep(1100 * time.Millisecond)
	// Acquire the cadence lock manually first.
	ok, _ := s.deps.Lock.Acquire(ctx, "BTC/USD")
	if !ok {
		t.Fatal("expected lock acquire")
	}
	s.tick(ctx)
	// Batch should remain open because lock was held.
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchOpen {
		t.Fatalf("expected open (lock held), got %s", got.Status)
	}
}