package clients

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- HTTP contract tests ---

func TestHTTPLiquidity_SubmitAggregate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/aggregate-orders" {
			t.Errorf("path=%s want /v1/aggregate-orders", r.URL.Path)
		}
		if key := r.Header.Get("Idempotency-Key"); key != "k1" {
			t.Errorf("idem key=%q want k1", key)
		}
		var req AggregateOrderRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.AssetPair != "BTC/USD" {
			t.Errorf("pair=%s want BTC/USD", req.AssetPair)
		}
		_ = json.NewEncoder(w).Encode(FillResult{FillPrice: 50000, TotalFilled: 1, VenueRoutes: []VenueRoute{{Venue: "v", Share: 1, Price: 50000}}})
	}))
	defer srv.Close()
	c := NewHTTPLiquidity(srv.URL)
	fill, err := c.SubmitAggregate(context.Background(), AggregateOrderRequest{AssetPair: "BTC/USD", Side: "buy", NotionalUSD: 50000}, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if fill.FillPrice != 50000 {
		t.Fatalf("fill_price=%f want 50000", fill.FillPrice)
	}
}

func TestHTTPFX_SubmitExposure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/hedges" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(HedgeResult{HedgedNotional: 40000, Unhedged: 10000})
	}))
	defer srv.Close()
	c := NewHTTPFX(srv.URL)
	res, err := c.SubmitExposure(context.Background(), HedgeRequest{FiatCurrency: "USD", NotionalUSD: 50000}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if res.HedgedNotional != 40000 {
		t.Fatalf("hedged=%f want 40000", res.HedgedNotional)
	}
}

func TestHTTPWallet_Fund(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/funding-moves" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(FundingMoveResult{Completed: true, TxID: "tx"})
	}))
	defer srv.Close()
	c := NewHTTPWallet(srv.URL)
	res, err := c.Fund(context.Background(), FundingMoveRequest{WalletID: "h", Asset: "BTC", Amount: 1}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed {
		t.Fatal("expected completed")
	}
}

func TestHTTPLedger_Post(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/postings" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	if err := c.Post(context.Background(), LedgerPost{Aggregate: "batch", EventType: "batch.close"}, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPAudit_Emit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audit-events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewHTTPAudit(srv.URL)
	if err := c.Emit(context.Background(), AuditEvent{Aggregate: "batch", EventType: "batch.close"}, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP_ConflictAndBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("dup"))
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestHTTP_UnavailableError(t *testing.T) {
	// Use a closed server to force a connection error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Fake tests ---

func TestFakeLiquidity_DuplicateKeyError(t *testing.T) {
	f := NewFakeLiquidity(FillResult{FillPrice: 1, TotalFilled: 1})
	f.SetDuplicateKeyError(true)
	if _, err := f.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestFakeLedger_DuplicateKeyError(t *testing.T) {
	f := NewFakeLedger()
	f.SetDuplicateKeyError(true)
	if err := f.Post(context.Background(), LedgerPost{}, "k"); err != nil {
		t.Fatal(err)
	}
	if err := f.Post(context.Background(), LedgerPost{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}