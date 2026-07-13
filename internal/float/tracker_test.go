package float

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func newTracker(t *testing.T, cfg config.Config) (*Tracker, *memstore.FloatStore) {
	t.Helper()
	all := memstore.NewAll()
	tr := New(Deps{Cfg: cfg, Floats: all.Float})
	return tr, all.Float
}

func TestTracker_OnAggregateFillIncrementsFloat(t *testing.T) {
	ctx := context.Background()
	tr, floats := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2}})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: 1, BatchID: 1, NotionalUSD: 50000, TotalFilled: 1}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	pos, _ := floats.GetFloat(ctx, "USD")
	if pos.ShortFiatAmount != 50000 {
		t.Fatalf("short=%f want 50000", pos.ShortFiatAmount)
	}
	if pos.LongCryptoAmount != 1 {
		t.Fatalf("long=%f want 1", pos.LongCryptoAmount)
	}
	if pos.LongCryptoAsset != "BTC" {
		t.Fatalf("asset=%s want BTC", pos.LongCryptoAsset)
	}
}

func TestTracker_SettlementDueAtIsTPlusN(t *testing.T) {
	ctx := context.Background()
	tr, floats := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2, "JPY": 3}})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/JPY"}
	order := &store.AggregateOrder{ID: 1, BatchID: 1, NotionalUSD: 1000, TotalFilled: 0.1}
	if err := tr.OnAggregateFill(ctx, batch, order, "JPY", "BTC"); err != nil {
		t.Fatal(err)
	}
	matured, _ := floats.ListMaturedFloat(ctx, time.Now().Add(2*24*time.Hour+time.Hour))
	if len(matured) != 0 {
		t.Fatalf("expected 0 matured before T+3, got %d", len(matured))
	}
	matured2, _ := floats.ListMaturedFloat(ctx, time.Now().Add(3*24*time.Hour+time.Hour))
	if len(matured2) != 1 {
		t.Fatalf("expected 1 matured at T+3, got %d", len(matured2))
	}
}

func TestTracker_BreachMaxTriggersAlert(t *testing.T) {
	ctx := context.Background()
	var breaches []string
	all := memstore.NewAll()
	tr := New(Deps{
		Cfg:    config.Config{MaxFloatUSD: 100000, SettlementDays: map[string]int{"USD": 2}},
		Floats: all.Float,
		OnBreach: func(ctx context.Context, fiat string, amount float64, bound string) {
			breaches = append(breaches, fiat+":"+bound)
		},
	})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: 1, BatchID: 1, NotionalUSD: 200000, TotalFilled: 4}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	if len(breaches) != 1 || breaches[0] != "USD:max" {
		t.Fatalf("breaches=%v want [USD:max]", breaches)
	}
}

func TestTracker_BreachMinTriggersAlert(t *testing.T) {
	ctx := context.Background()
	var breaches []string
	all := memstore.NewAll()
	tr := New(Deps{
		Cfg:    config.Config{MinFloatUSD: 300000, SettlementDays: map[string]int{"USD": 2}},
		Floats: all.Float,
		OnBreach: func(ctx context.Context, fiat string, amount float64, bound string) {
			breaches = append(breaches, fiat+":"+bound)
		},
	})
	batch := &store.Batch{ID: 1, AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: 1, BatchID: 1, NotionalUSD: 1000, TotalFilled: 0.01}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	if len(breaches) != 1 || breaches[0] != "USD:min" {
		t.Fatalf("breaches=%v want [USD:min]", breaches)
	}
}

func TestTracker_SweepMaturedSettles(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	tr := New(Deps{Cfg: config.Config{}, Floats: all.Float})
	past := time.Now().UTC().Add(-1 * time.Hour)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: 1000, LongCryptoAmount: 0.02, LongCryptoAsset: "BTC", SettlementDueAt: past})
	n, err := tr.SweepMatured(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("swept=%d want 1", n)
	}
	matured, _ := all.Float.ListMaturedFloat(ctx, time.Now())
	if len(matured) != 0 {
		t.Fatalf("expected 0 matured after sweep, got %d", len(matured))
	}
}