package api

import (
	"bytes"
	"context"
	"encoding/json"
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