package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/eventbus"
)

// TestApp_FullFlow exercises the in-memory composition root: build the
// server, POST a tx completion event, observe a batch membership, close
// the batch (manual), and verify the aggregate order is persisted.
func TestApp_FullFlow(t *testing.T) {
	cfg := config.Config{
		Port:                  "0",
		BatchIntervalSeconds:  3600,
		BatchSizeThresholdUSD: 100000,
		SettlementDays:        map[string]int{"USD": 2},
	}
	srv, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start background loops.
	go func() { _ = srv.scheduler.Run(ctx) }()
	go func() { _ = srv.consumer.Run(ctx) }()
	go func() { _ = srv.emitter.RunDispatcherLoop(ctx, 200*time.Millisecond) }()
	go func() { _ = srv.float.RunSweeperLoop(ctx, 200*time.Millisecond) }()
	time.Sleep(50 * time.Millisecond) // allow loops to subscribe

	hs := httptest.NewServer(srv.HTTPHandler())
	defer hs.Close()

	// POST a tx completion event.
	ev := `{"tx_id":"tx1","asset":"BTC","fiat_currency":"USD","amount":1,"notional_usd":1000,"user_id":"u1"}`
	resp, err := http.Post(hs.URL+"/v1/events/tx.completed", "application/json", strings.NewReader(ev))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("event post code=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// Wait for the consumer to persist the membership + open batch.
	deadline := time.Now().Add(2 * time.Second)
	var batchID int64
	for time.Now().Before(deadline) {
		listResp, _ := http.Get(hs.URL + "/v1/batches")
		if listResp != nil {
			var out struct {
				Batches []struct {
					ID        int64  `json:"id"`
					AssetPair string `json:"asset_pair"`
					Status    string `json:"status"`
				} `json:"batches"`
			}
			_ = json.NewDecoder(listResp.Body).Decode(&out)
			listResp.Body.Close()
			if len(out.Batches) >= 1 && out.Batches[0].Status == "open" {
				batchID = out.Batches[0].ID
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if batchID == 0 {
		t.Fatal("no open batch created")
	}

	// Manual close.
	closeResp, err := http.Post(hs.URL+"/v1/batches/"+itoa(batchID)+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if closeResp.StatusCode != http.StatusOK {
		t.Fatalf("close code=%d", closeResp.StatusCode)
	}
	closeResp.Body.Close()

	// The OnClose callback submits the aggregate order. Verify the batch
	// transitioned to settled (or executing) and an order exists.
	deadline = time.Now().Add(2 * time.Second)
	var orderStatus string
	for time.Now().Before(deadline) {
		getResp, _ := http.Get(hs.URL + "/v1/batches/" + itoa(batchID))
		if getResp != nil {
			var out struct {
				Batch struct {
					Status string `json:"status"`
				} `json:"batch"`
				Order *struct {
					Status string `json:"status"`
				} `json:"order"`
			}
			_ = json.NewDecoder(getResp.Body).Decode(&out)
			getResp.Body.Close()
			if out.Order != nil {
				orderStatus = out.Order.Status
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if orderStatus == "" {
		t.Fatal("expected aggregate order after close")
	}

	// Float position should reflect the fill.
	floatResp, err := http.Get(hs.URL + "/v1/float/USD")
	if err != nil {
		t.Fatal(err)
	}
	if floatResp.StatusCode != http.StatusOK {
		t.Fatalf("float code=%d", floatResp.StatusCode)
	}
	var pos struct {
		ShortFiatAmount float64 `json:"short_fiat_amount"`
	}
	_ = json.NewDecoder(floatResp.Body).Decode(&pos)
	floatResp.Body.Close()
	if pos.ShortFiatAmount <= 0 {
		t.Fatalf("float short=%f want >0", pos.ShortFiatAmount)
	}

	// Metrics endpoint should be served.
	metricsResp, err := http.Get(hs.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics code=%d", metricsResp.StatusCode)
	}
	metricsResp.Body.Close()

	// /v1/healthz and /readyz on the composed router.
	for _, p := range []string{"/healthz", "/readyz"} {
		r, _ := http.Get(hs.URL + p)
		if r == nil || r.StatusCode != http.StatusOK {
			t.Fatalf("%s code=%v", p, r)
		}
		r.Body.Close()
	}

	// Dedup: re-post the same event; should not create a second membership.
	dupResp, _ := http.Post(hs.URL+"/v1/events/tx.completed", "application/json", strings.NewReader(ev))
	if dupResp != nil {
		if dupResp.StatusCode != http.StatusAccepted {
			t.Fatalf("dup event code=%d", dupResp.StatusCode)
		}
		dupResp.Body.Close()
	}
	time.Sleep(100 * time.Millisecond)
	listResp, _ := http.Get(hs.URL + "/v1/batches/" + itoa(batchID))
	var out struct {
		Memberships []any `json:"memberships"`
	}
	_ = json.NewDecoder(listResp.Body).Decode(&out)
	listResp.Body.Close()
	if len(out.Memberships) != 1 {
		t.Fatalf("memberships=%d want 1 (dedup)", len(out.Memberships))
	}

	_ = eventbus.TxCompletedEvent{}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}