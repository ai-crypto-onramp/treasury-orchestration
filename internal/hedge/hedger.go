// Package hedge forwards net aggregate FX exposure to the fx-hedging
// service after each aggregate fill. It is idempotent on batch_id and
// emits a Prometheus gauge for unhedged exposure per currency.
package hedge

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/metrics"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
)

// Deps bundles the hedger dependencies.
type Deps struct {
	FX     clients.FXHedging
	Orders store.AggregateOrderStore
	Idem   idempotency.Store
}

// Hedger computes and submits FX exposure per fiat currency.
type Hedger struct {
	deps Deps
}

// New returns a new hedger.
func New(deps Deps) *Hedger { return &Hedger{deps: deps} }

// OnAggregateFill computes the net FX exposure (short fiat delta) and
// submits it to fx-hedging. Persists hedged_notional on the aggregate
// order. Idempotent on batch_id.
func (h *Hedger) OnAggregateFill(ctx context.Context, batch *store.Batch, order *store.AggregateOrder, fiatCurrency string) (*store.AggregateOrder, error) {
	key := fmt.Sprintf("hedge:%s", batch.ID)
	ok, err := h.deps.Idem.CheckAndMark(ctx, key, 24*time.Hour)
	if err != nil {
		log.Printf("hedge: idem check batch=%s: %v", batch.ID, err)
	}
	if err == nil && !ok {
		// Replay; do not double-hedge.
		log.Printf("hedge: dup batch=%s skipped", batch.ID)
		return order, nil
	}
	exposure := order.NotionalUSD
	if exposure <= 0 {
		return order, nil
	}
	res, err := h.deps.FX.SubmitExposure(ctx, clients.HedgeRequest{
		FiatCurrency: fiatCurrency,
		NotionalUSD:  exposure,
		BatchID:      batch.ID,
	}, key)
	if err != nil {
		log.Printf("hedge: submit batch=%s: %v", batch.ID, err)
		metrics.UnhedgedExposure.WithLabelValues(fiatCurrency).Add(exposure)
		return nil, err
	}
	hedged := 0.0
	if res != nil {
		hedged = res.HedgedNotional
	}
	updated, err := h.deps.Orders.SettleOrder(ctx, batch.ID, hedged)
	if err != nil {
		return nil, err
	}
	unhedged := exposure - hedged
	metrics.UnhedgedExposure.WithLabelValues(fiatCurrency).Set(unhedged)
	log.Printf("hedge: batch=%s fiat=%s exposure=%.2f hedged=%.2f unhedged=%.2f", batch.ID, fiatCurrency, exposure, hedged, unhedged)
	return updated, nil
}