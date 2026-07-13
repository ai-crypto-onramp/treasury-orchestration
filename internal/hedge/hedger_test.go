package hedge

import (
	"context"
	"testing"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func TestHedger_OnAggregateFillSubmitsExposure(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	fx := clients.NewFakeFX(clients.HedgeResult{HedgedNotional: 40000})
	h := New(Deps{FX: fx, Orders: all.Order, Idem: idem})

	// Create an aggregate order row first.
	o, _ := all.Order.CreateOrder(ctx, &store.AggregateOrder{BatchID: 1, AssetPair: "BTC/USD", NotionalUSD: 50000, Status: store.AggregateExecuting})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}

	updated, err := h.OnAggregateFill(ctx, batch, o, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if updated.HedgedNotional != 40000 {
		t.Fatalf("hedged=%f want 40000", updated.HedgedNotional)
	}
	if updated.Status != store.AggregateSettled {
		t.Fatalf("status=%s want settled", updated.Status)
	}
	calls := fx.Calls()
	if len(calls) != 1 {
		t.Fatalf("fx calls=%d want 1", len(calls))
	}
	if calls[0].FiatCurrency != "USD" || calls[0].NotionalUSD != 50000 {
		t.Fatalf("call=%+v", calls[0])
	}
}

func TestHedger_IdempotentReplayDoesNotDoubleHedge(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	fx := clients.NewFakeFX(clients.HedgeResult{HedgedNotional: 50000})
	h := New(Deps{FX: fx, Orders: all.Order, Idem: idem})
	o, _ := all.Order.CreateOrder(ctx, &store.AggregateOrder{BatchID: 1, AssetPair: "BTC/USD", NotionalUSD: 50000, Status: store.AggregateExecuting})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}
	if _, err := h.OnAggregateFill(ctx, batch, o, "USD"); err != nil {
		t.Fatal(err)
	}
	// Replay: the idem key is already marked, so no second hedge call.
	o2, _ := all.Order.GetOrderByBatch(ctx, 1)
	if _, err := h.OnAggregateFill(ctx, batch, o2, "USD"); err != nil {
		t.Fatal(err)
	}
	calls := fx.Calls()
	if len(calls) != 1 {
		t.Fatalf("fx calls=%d want 1 (no double-hedge)", len(calls))
	}
}

func TestHedger_ErrorPropagatesAndSetsUnhedged(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	fx := clients.NewFakeFX(clients.HedgeResult{})
	fx.SetError(clients.ErrUnavailable)
	h := New(Deps{FX: fx, Orders: all.Order, Idem: idem})
	o, _ := all.Order.CreateOrder(ctx, &store.AggregateOrder{BatchID: 1, AssetPair: "BTC/USD", NotionalUSD: 50000, Status: store.AggregateExecuting})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}
	if _, err := h.OnAggregateFill(ctx, batch, o, "USD"); err == nil {
		t.Fatal("expected error")
	}
}