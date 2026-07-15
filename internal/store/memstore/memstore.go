// Package memstore is an in-memory implementation of the store interfaces,
// used by unit tests and the in-memory run mode so `go test ./...` passes
// without requiring Docker or a live Postgres. It is safe for concurrent
// use.
package memstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
)

// All is a composite of all in-memory stores.
type All struct {
	Batch     *BatchStore
	Membership *MembershipStore
	Order     *AggregateOrderStore
	Funding   *FundingStore
	Float     *FloatStore
	Rebalance *RebalancingStore
	Outbox    *OutboxStore
}

// NewAll returns a fully wired set of in-memory stores.
func NewAll() *All {
	return &All{
		Batch:      NewBatchStore(),
		Membership: NewMembershipStore(),
		Order:      NewAggregateOrderStore(),
		Funding:    NewFundingStore(),
		Float:      NewFloatStore(),
		Rebalance:  NewRebalancingStore(),
		Outbox:     NewOutboxStore(),
	}
}

// --- BatchStore ---

type BatchStore struct {
	mu      sync.Mutex
	rows    []*store.Batch
	nextID  int64
}

func NewBatchStore() *BatchStore { return &BatchStore{} }

func (s *BatchStore) OpenBatch(_ context.Context, assetPair string) (*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.AssetPair == assetPair && b.Status == store.BatchOpen {
			c := *b
			return &c, nil
		}
	}
	s.nextID++
	b := &store.Batch{
		ID:        s.nextID,
		AssetPair: assetPair,
		Status:    store.BatchOpen,
		OpenedAt:  time.Now().UTC(),
	}
	s.rows = append(s.rows, b)
	c := *b
	return &c, nil
}

func (s *BatchStore) GetBatch(_ context.Context, id int64) (*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			c := *b
			return &c, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *BatchStore) ListBatches(_ context.Context, from, to time.Time) ([]*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Batch
	for _, b := range s.rows {
		if !from.IsZero() && b.OpenedAt.Before(from) {
			continue
		}
		if !to.IsZero() && b.OpenedAt.After(to) {
			continue
		}
		c := *b
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.Before(out[j].OpenedAt) })
	return out, nil
}

func (s *BatchStore) ListOpenBatches(_ context.Context) ([]*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Batch
	for _, b := range s.rows {
		if b.Status == store.BatchOpen {
			c := *b
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *BatchStore) UpdateBatchStatus(_ context.Context, id int64, from, to store.BatchStatus, mutator func(*store.Batch)) (*store.Batch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			if b.Status != from {
				return nil, false, nil
			}
			if !from.CanTransitionTo(to) {
				return nil, false, store.ErrConflict
			}
			b.Status = to
			if to == store.BatchClosed && b.ClosedAt.IsZero() {
				b.ClosedAt = time.Now().UTC()
			}
			if mutator != nil {
				mutator(b)
			}
			c := *b
			return &c, true, nil
		}
	}
	return nil, false, store.ErrNotFound
}

func (s *BatchStore) SetBatchNotional(_ context.Context, id int64, notional float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			b.NotionalUSD = notional
			return nil
		}
	}
	return store.ErrNotFound
}

// --- MembershipStore ---

type MembershipStore struct {
	mu     sync.Mutex
	rows   []*store.Membership
	nextID int64
	byTx   map[string]bool
}

func NewMembershipStore() *MembershipStore {
	return &MembershipStore{byTx: map[string]bool{}}
}

func (s *MembershipStore) AddMembership(_ context.Context, m *store.Membership) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTx[m.TxID] {
		return false, nil
	}
	s.byTx[m.TxID] = true
	s.nextID++
	c := *m
	c.ID = s.nextID
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return true, nil
}

func (s *MembershipStore) ListMemberships(_ context.Context, batchID int64) ([]*store.Membership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Membership
	for _, m := range s.rows {
		if m.BatchID == batchID {
			c := *m
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MembershipStore) SumNotional(_ context.Context, batchID int64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sum float64
	for _, m := range s.rows {
		if m.BatchID == batchID {
			sum += m.NotionalUSD
		}
	}
	return sum, nil
}

func (s *MembershipStore) ExistsByTxID(_ context.Context, txID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byTx[txID], nil
}

// --- AggregateOrderStore ---

type AggregateOrderStore struct {
	mu     sync.Mutex
	rows   []*store.AggregateOrder
	nextID int64
}

func NewAggregateOrderStore() *AggregateOrderStore { return &AggregateOrderStore{} }

func (s *AggregateOrderStore) CreateOrder(_ context.Context, o *store.AggregateOrder) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == o.BatchID {
			c := *r
			return &c, nil
		}
	}
	s.nextID++
	c := *o
	c.ID = s.nextID
	if c.Side == "" {
		c.Side = "buy"
	}
	if c.Status == "" {
		c.Status = store.AggregateExecuting
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *AggregateOrderStore) GetOrderByBatch(_ context.Context, batchID int64) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			c := *r
			return &c, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *AggregateOrderStore) ListOrders(_ context.Context, status string) ([]*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.AggregateOrder
	for _, r := range s.rows {
		if status != "" && string(r.Status) != status {
			continue
		}
		c := *r
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *AggregateOrderStore) UpdateOrderFill(_ context.Context, batchID int64, fillPrice, totalFilled float64, venueRoutes []store.VenueRoute) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			r.FillPrice = fillPrice
			r.TotalFilled = totalFilled
			r.VenueRoutes = venueRoutes
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *AggregateOrderStore) SettleOrder(_ context.Context, batchID int64, hedgedNotional float64) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			r.Status = store.AggregateSettled
			r.HedgedNotional = hedgedNotional
			r.SettledAt = time.Now().UTC()
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

// --- FundingStore ---

type FundingStore struct {
	mu     sync.Mutex
	rows   []*store.FundingRequest
	nextID int64
}

func NewFundingStore() *FundingStore { return &FundingStore{} }

func (s *FundingStore) CreateFunding(_ context.Context, f *store.FundingRequest) (*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	c := *f
	c.ID = s.nextID
	if c.Status == "" {
		c.Status = store.FundingPending
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *FundingStore) GetFunding(_ context.Context, id int64) (*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *FundingStore) UpdateFundingStatus(_ context.Context, id int64, status store.FundingStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Status = status
			if status == store.FundingCompleted || status == store.FundingRejected {
				r.CompletedAt = time.Now().UTC()
			}
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *FundingStore) ListFunding(_ context.Context, status string) ([]*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.FundingRequest
	for _, r := range s.rows {
		if status == "" || string(r.Status) == status {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// --- FloatStore ---

type FloatStore struct {
	mu     sync.Mutex
	rows   []*store.FloatPosition
	nextID int64
}

func NewFloatStore() *FloatStore { return &FloatStore{} }

func (s *FloatStore) AddFloat(_ context.Context, p *store.FloatPosition) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Accumulate into the latest unsettled row for the currency.
	for _, r := range s.rows {
		if r.FiatCurrency == p.FiatCurrency && !r.Settled {
			r.ShortFiatAmount += p.ShortFiatAmount
			r.LongCryptoAmount += p.LongCryptoAmount
			if p.LongCryptoAsset != "" {
				r.LongCryptoAsset = p.LongCryptoAsset
			}
			if !p.SettlementDueAt.IsZero() {
				r.SettlementDueAt = p.SettlementDueAt
			}
			r.UpdatedAt = time.Now().UTC()
			return &*r, nil
		}
	}
	s.nextID++
	c := *p
	c.ID = s.nextID
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	c.UpdatedAt = time.Now().UTC()
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *FloatStore) GetFloat(_ context.Context, fiatCurrency string) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var agg *store.FloatPosition
	for _, r := range s.rows {
		if r.FiatCurrency != fiatCurrency {
			continue
		}
		if agg == nil {
			agg = &store.FloatPosition{FiatCurrency: fiatCurrency, LongCryptoAsset: r.LongCryptoAsset}
		}
		agg.ShortFiatAmount += r.ShortFiatAmount
		agg.LongCryptoAmount += r.LongCryptoAmount
		if r.SettlementDueAt.After(agg.SettlementDueAt) {
			agg.SettlementDueAt = r.SettlementDueAt
		}
	}
	if agg == nil {
		return &store.FloatPosition{FiatCurrency: fiatCurrency}, nil
	}
	return agg, nil
}

func (s *FloatStore) ListFloat(_ context.Context) ([]*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byCcy := map[string]*store.FloatPosition{}
	var order []string
	for _, r := range s.rows {
		agg, ok := byCcy[r.FiatCurrency]
		if !ok {
			agg = &store.FloatPosition{FiatCurrency: r.FiatCurrency, LongCryptoAsset: r.LongCryptoAsset}
			byCcy[r.FiatCurrency] = agg
			order = append(order, r.FiatCurrency)
		}
		agg.ShortFiatAmount += r.ShortFiatAmount
		agg.LongCryptoAmount += r.LongCryptoAmount
		if r.LongCryptoAsset != "" {
			agg.LongCryptoAsset = r.LongCryptoAsset
		}
		if r.SettlementDueAt.After(agg.SettlementDueAt) {
			agg.SettlementDueAt = r.SettlementDueAt
		}
	}
	sort.Strings(order)
	out := make([]*store.FloatPosition, 0, len(order))
	for _, c := range order {
		out = append(out, byCcy[c])
	}
	return out, nil
}

func (s *FloatStore) ListMaturedFloat(_ context.Context, before time.Time) ([]*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.FloatPosition
	for _, r := range s.rows {
		if r.Settled {
			continue
		}
		if r.SettlementDueAt.IsZero() {
			continue
		}
		if !r.SettlementDueAt.After(before) {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *FloatStore) SettleFloat(_ context.Context, id int64) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Settled = true
			r.UpdatedAt = time.Now().UTC()
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

// --- RebalancingStore ---

type RebalancingStore struct {
	mu     sync.Mutex
	rows   []*store.RebalancingJob
	nextID int64
}

func NewRebalancingStore() *RebalancingStore { return &RebalancingStore{} }

func (s *RebalancingStore) CreateJob(_ context.Context, j *store.RebalancingJob) (*store.RebalancingJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	c := *j
	c.ID = s.nextID
	if c.Status == "" {
		c.Status = store.RebalancePending
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *RebalancingStore) ListJobs(_ context.Context, status string) ([]*store.RebalancingJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.RebalancingJob
	for _, r := range s.rows {
		if status == "" || string(r.Status) == status {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *RebalancingStore) UpdateJobStatus(_ context.Context, id int64, status store.RebalanceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Status = status
			if status == store.RebalanceCompleted || status == store.RebalanceRejected {
				r.CompletedAt = time.Now().UTC()
			}
			return nil
		}
	}
	return store.ErrNotFound
}

// --- OutboxStore ---

type OutboxStore struct {
	mu     sync.Mutex
	rows   []*store.OutboxEntry
	seen   map[string]bool
	nextID int64
}

func NewOutboxStore() *OutboxStore { return &OutboxStore{seen: map[string]bool{}} }

func (s *OutboxStore) Append(_ context.Context, e *store.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[e.DedupKey] {
		return false, nil
	}
	s.seen[e.DedupKey] = true
	s.nextID++
	c := *e
	c.ID = s.nextID
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return true, nil
}

func (s *OutboxStore) ListPending(_ context.Context, limit int) ([]*store.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.OutboxEntry
	for _, r := range s.rows {
		if r.EmittedAt.IsZero() {
			c := *r
			out = append(out, &c)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *OutboxStore) MarkEmitted(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.EmittedAt = time.Now().UTC()
			return nil
		}
	}
	return store.ErrNotFound
}

// Snapshot returns a copy of all outbox entries (test helper).
func (s *OutboxStore) Snapshot() []*store.OutboxEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*store.OutboxEntry, len(s.rows))
	for i, r := range s.rows {
		c := *r
		out[i] = &c
	}
	return out
}