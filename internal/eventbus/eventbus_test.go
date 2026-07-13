package eventbus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInMemory_PushAndSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewInMemory()
	ch, stop, err := bus.Subscribe(ctx, "tx.completed")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ev := TxCompletedEvent{TxID: "tx1", Asset: "BTC", FiatCurrency: "USD"}
	if err := bus.Push(ctx, "tx.completed", ev); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.TxID != "tx1" {
			t.Fatalf("got tx=%s want tx1", got.TxID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestInMemory_UnsubscribeStopsChannel(t *testing.T) {
	ctx := context.Background()
	bus := NewInMemory()
	ch, stop, _ := bus.Subscribe(ctx, "tx.completed")
	stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("expected channel closed")
	}
}

func TestInMemory_TopicIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewInMemory()
	ch, stop, _ := bus.Subscribe(ctx, "tx.completed")
	defer stop()
	_ = bus.Push(ctx, "other.topic", TxCompletedEvent{TxID: "x"})
	select {
	case <-ch:
		t.Fatal("expected no event on different topic")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHTTPPush_Handler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHTTPPush()
	ch, stop, _ := h.Subscribe(ctx, "tx.completed")
	defer stop()
	srv := httptest.NewServer(h.HTTPHandler())
	defer srv.Close()
	body := `{"tx_id":"tx2","asset":"ETH","fiat_currency":"USD","amount":1,"notional_usd":2000}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/events/tx.completed", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d want 202", resp.StatusCode)
	}
	select {
	case got := <-ch:
		if got.TxID != "tx2" {
			t.Fatalf("got tx=%s want tx2", got.TxID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestHTTPPush_MethodNotAllowed(t *testing.T) {
	h := NewHTTPPush()
	req := httptest.NewRequest(http.MethodGet, "/v1/events/tx.completed", nil)
	rec := httptest.NewRecorder()
	h.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", rec.Code)
	}
}