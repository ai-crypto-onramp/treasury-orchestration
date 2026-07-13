package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/batch"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/funding"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/projection"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/float"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func newDeps(t *testing.T) (*Deps, *memstore.All, *batch.Scheduler) {
	t.Helper()
	all := memstore.NewAll()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: 50000, SettlementDays: map[string]int{"USD": 2}}
	lock := idempotency.NewCadenceLock(idempotency.NewMem(), time.Duration(cfg.BatchIntervalSeconds)*time.Second)
	sched := batch.New(batch.Deps{Cfg: cfg, Batches: all.Batch, Memberships: all.Membership, Lock: lock})
	fMgr := funding.New(funding.Deps{
		Cfg:        cfg,
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:       idempotency.NewMem(),
		Projection: projection.New(time.Minute),
	})
	fTracker := float.New(float.Deps{Cfg: cfg, Floats: all.Float})
	return &Deps{
		Batches:   all.Batch,
		Members:   all.Membership,
		Orders:    all.Order,
		Scheduler: sched,
		Float:     fTracker,
		Funding:   fMgr,
	}, all, sched
}

func TestHealthz(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
}

func TestReadyz(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
}

func TestBatches_ListAndCreate(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", Asset: "BTC", FiatCurrency: "USD", NotionalUSD: 1000})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		Batches []*store.Batch `json:"batches"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Batches) != 1 {
		t.Fatalf("batches=%d want 1", len(out.Batches))
	}
}

func TestBatchByID_GetWithMemberships(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", Asset: "BTC", FiatCurrency: "USD", NotionalUSD: 1000})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + itoa(b.ID))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		Batch      *store.Batch            `json:"batch"`
		Memberships []*store.Membership    `json:"memberships"`
		Order      *store.AggregateOrder   `json:"order"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Batch == nil || out.Batch.ID != b.ID {
		t.Fatalf("batch=%+v", out.Batch)
	}
	if len(out.Memberships) != 1 {
		t.Fatalf("members=%d want 1", len(out.Memberships))
	}
}

func TestBatchByID_NotFound(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/9999")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("code=%d want 404", resp.StatusCode)
	}
}

func TestBatchByID_Close(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+itoa(b.ID)+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", got.Status)
	}
}

func TestFloat_Get(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: 1234, LongCryptoAmount: 0.02, LongCryptoAsset: "BTC"})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/float/USD")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var pos store.FloatPosition
	_ = json.NewDecoder(resp.Body).Decode(&pos)
	if pos.ShortFiatAmount != 1234 {
		t.Fatalf("short=%f want 1234", pos.ShortFiatAmount)
	}
}

func TestFunding_Create(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"hot1","asset":"BTC","amount":5,"source_venue":"v"}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var fr store.FundingRequest
	_ = json.NewDecoder(resp.Body).Decode(&fr)
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
}

func TestFunding_Create_InvalidAmount(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"hot1","asset":"BTC","amount":0}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_List(t *testing.T) {
	ctx := context.Background()
	d, _, _ := newDeps(t)
	_, _ = d.Funding.CreateFundingRequest(ctx, "h1", "BTC", 1, "")
	_, _ = d.Funding.CreateFundingRequest(ctx, "h2", "ETH", 2, "")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/funding-requests?status=completed")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		FundingRequests []*store.FundingRequest `json:"funding_requests"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.FundingRequests) != 2 {
		t.Fatalf("funding=%d want 2", len(out.FundingRequests))
	}
}

func TestRebalancingJobs_List(t *testing.T) {
	ctx := context.Background()
	d, _, _ := newDeps(t)
	_, _ = d.Funding.Rebalance(ctx, "a", "b", "BTC", 1, "drift")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/rebalancing-jobs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		RebalancingJobs []*store.RebalancingJob `json:"rebalancing_jobs"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.RebalancingJobs) != 1 {
		t.Fatalf("jobs=%d want 1", len(out.RebalancingJobs))
	}
}

func TestFunding_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/funding-requests", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

// itoa avoids strconv for small ids in test helpers.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// --- additional coverage ---

func TestBatches_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/batches", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatches_ListBatchesError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Batches = errBatchStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestBatchByID_MissingID(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	// "/v1/batches/" with empty id.
	resp, err := http.Get(srv.URL + "/v1/batches/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestBatchByID_InvalidID(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/notanumber")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestBatchByID_CloseMethodNotAllowed(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/batches/"+itoa(b.ID)+"/close", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatchByID_CloseSchedulerNil(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	d.Scheduler = nil
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+itoa(b.ID)+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("code=%d want 503", resp.StatusCode)
	}
}

func TestBatchByID_CloseAlreadyClosedConflict(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	// Close it once first.
	if _, err := d.Scheduler.CloseBatch(context.Background(), b.ID); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+itoa(b.ID)+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("code=%d want 409", resp.StatusCode)
	}
}

func TestBatchByID_CloseNotFound(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/9999/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("code=%d want 404", resp.StatusCode)
	}
}

func TestBatchByID_GetInternalError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Batches = errBatchStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestBatchByID_GetWithoutOrdersOrMembers(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	d.Orders = nil
	d.Members = nil
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + itoa(b.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

func TestFloat_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/float/USD", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestFloat_MissingCurrency(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	// "/v1/float/" with empty currency.
	resp, err := http.Get(srv.URL + "/v1/float/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_MalformedJSON(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", strings.NewReader("{bad"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_PolicyViolation(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"h","asset":"BTC","amount":` + itoa(int64(11_000_000)) + `}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("code=%d want 403", resp.StatusCode)
	}
}

func TestFunding_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	// Replace Funding with one backed by an erroring store.
	d.Funding = funding.New(funding.Deps{
		Cfg:     config.Config{},
		Funding: errFundingStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/funding-requests")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestRebalancing_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/rebalancing-jobs", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestRebalancing_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Funding = funding.New(funding.Deps{
		Cfg:       config.Config{},
		Rebalance: errRebalStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/rebalancing-jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestNewRouter_NilDepsOmitsRoutes(t *testing.T) {
	// A Deps with no Batches/Float/Funding should still serve healthz.
	srv := httptest.NewServer(NewRouter(&Deps{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

// --- fakes ---

var errAPI = errors.New("api store boom")

type errBatchStore struct{ err error }

func (e errBatchStore) OpenBatch(context.Context, string) (*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) GetBatch(context.Context, int64) (*store.Batch, error) { return nil, e.err }
func (e errBatchStore) ListBatches(context.Context, time.Time, time.Time) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) ListOpenBatches(context.Context) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) UpdateBatchStatus(context.Context, int64, store.BatchStatus, store.BatchStatus, func(*store.Batch)) (*store.Batch, bool, error) {
	return nil, false, e.err
}
func (e errBatchStore) SetBatchNotional(context.Context, int64, float64) error { return e.err }

type errFundingStore struct{ err error }

func (e errFundingStore) CreateFunding(context.Context, *store.FundingRequest) (*store.FundingRequest, error) {
	return nil, e.err
}
func (e errFundingStore) GetFunding(context.Context, int64) (*store.FundingRequest, error) {
	return nil, e.err
}
func (e errFundingStore) UpdateFundingStatus(context.Context, int64, store.FundingStatus) error {
	return e.err
}
func (e errFundingStore) ListFunding(context.Context, string) ([]*store.FundingRequest, error) {
	return nil, e.err
}

type errRebalStore struct{ err error }

func (e errRebalStore) CreateJob(context.Context, *store.RebalancingJob) (*store.RebalancingJob, error) {
	return nil, e.err
}
func (e errRebalStore) ListJobs(context.Context, string) ([]*store.RebalancingJob, error) {
	return nil, e.err
}
func (e errRebalStore) UpdateJobStatus(context.Context, int64, store.RebalanceStatus) error {
	return e.err
}