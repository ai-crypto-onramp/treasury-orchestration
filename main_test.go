package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	want := `{"status":"ok"}` + "\n"
	if body != want {
		t.Fatalf("expected body %q, got %q", want, body)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}
}

func TestReady_Ok(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	readyHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var got map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got["status"] != "ready" {
		t.Fatalf("expected status 'ready', got %q", got["status"])
	}
	if got["healthy"] != "18" {
		t.Fatalf("expected healthy '18', got %q", got["healthy"])
	}
	if got["failed"] != "0" {
		t.Fatalf("expected failed '0', got %q", got["failed"])
	}
	if got["total"] != "18" {
		t.Fatalf("expected total '18', got %q", got["total"])
	}
	for _, name := range []string{"db", "mq", "signer", "pricing", "liquidity", "rail", "fx", "blockchain", "aml", "identity", "onboarding", "wallet", "notification", "policy", "audit", "ledger", "exchange", "mpc"} {
		if got[name] != "ok" {
			t.Fatalf("expected %s to be 'ok', got %q", name, got[name])
		}
	}
}

func TestClassifyReadiness(t *testing.T) {
	tests := []struct {
		name       string
		failed     int
		total      int
		wantCode   int
		wantStatus string
	}{
		{name: "all healthy", failed: 0, total: 18, wantCode: http.StatusOK, wantStatus: "ready"},
		{name: "some failed", failed: 3, total: 18, wantCode: http.StatusOK, wantStatus: "degraded"},
		{name: "zero checks", failed: 0, total: 0, wantCode: http.StatusOK, wantStatus: "ready"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, status := classifyReadiness(tt.failed, tt.total)
			if code != tt.wantCode {
				t.Fatalf("expected code %d, got %d", tt.wantCode, code)
			}
			if status != tt.wantStatus {
				t.Fatalf("expected status %q, got %q", tt.wantStatus, status)
			}
		})
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		in   int
		want string
	}{
		{in: 0, want: "0"},
		{in: 7, want: "7"},
		{in: 18, want: "18"},
		{in: 503, want: "503"},
	}
	for _, tt := range tests {
		if got := itoa(tt.in); got != tt.want {
			t.Fatalf("itoa(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestReadinessChecks(t *testing.T) {
	checks := readinessChecks()
	if len(checks) != 18 {
		t.Fatalf("expected 18 checks, got %d", len(checks))
	}
	for _, c := range checks {
		if !c.fn() {
			t.Fatalf("check %s returned false", c.name)
		}
	}
}

func TestReadinessReport(t *testing.T) {
	results, failed, total := readinessReport()
	if failed != 0 || total != 18 {
		t.Fatalf("expected failed=0 total=18, got failed=%d total=%d", failed, total)
	}
	if results["status"] != "" {
		t.Fatalf("report should not include status, got %q", results["status"])
	}
	if results["db"] != "ok" {
		t.Fatalf("expected db 'ok', got %q", results["db"])
	}
}

func TestReadinessReport_WithFailure(t *testing.T) {
	orig := readinessChecks
	t.Cleanup(func() { readinessChecks = orig })
	readinessChecks = func() []readinessCheck {
		return []readinessCheck{
			{name: "db", fn: func() bool { return true }},
			{name: "mq", fn: func() bool { return false }},
		}
	}
	results, failed, total := readinessReport()
	if failed != 1 || total != 2 {
		t.Fatalf("expected failed=1 total=2, got failed=%d total=%d", failed, total)
	}
	if results["db"] != "ok" {
		t.Fatalf("expected db 'ok', got %q", results["db"])
	}
	if results["mq"] != "down" {
		t.Fatalf("expected mq 'down', got %q", results["mq"])
	}
}

func TestNewMux(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantCode int
	}{
		{name: "healthz route", path: "/healthz", wantCode: http.StatusOK},
		{name: "unknown route", path: "/nope", wantCode: http.StatusNotFound},
	}

	mux := newMux()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tt.wantCode {
				t.Fatalf("GET %s: expected status %d, got %d", tt.path, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestResolvePort(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want string
	}{
		{name: "default when unset", env: "", want: "8080"},
		{name: "explicit port", env: "9090", want: "9090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PORT", tt.env)
			if got := resolvePort(); got != tt.want {
				t.Fatalf("resolvePort() = %q, want %q", got, tt.want)
			}
		})
	}
}

// stubListenAndServe replaces listenAndServe for the duration of a test and
// records the address it was invoked with.
func stubListenAndServe(t *testing.T, retErr error) *string {
	t.Helper()
	var gotAddr string
	orig := listenAndServe
	listenAndServe = func(addr string, handler http.Handler) error {
		gotAddr = addr
		if handler == nil {
			t.Error("listenAndServe called with nil handler")
		}
		return retErr
	}
	t.Cleanup(func() { listenAndServe = orig })
	return &gotAddr
}

func TestRun(t *testing.T) {
	tests := []struct {
		name     string
		env      string
		wantAddr string
		retErr   error
	}{
		{name: "default port", env: "", wantAddr: ":8080", retErr: nil},
		{name: "custom port", env: "9191", wantAddr: ":9191", retErr: errors.New("listen failure")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PORT", tt.env)
			gotAddr := stubListenAndServe(t, tt.retErr)

			err := run()

			if !errors.Is(err, tt.retErr) {
				t.Fatalf("run() error = %v, want %v", err, tt.retErr)
			}
			if *gotAddr != tt.wantAddr {
				t.Fatalf("run() listened on %q, want %q", *gotAddr, tt.wantAddr)
			}
		})
	}
}

func TestMain_InvokesServer(t *testing.T) {
	t.Setenv("PORT", "9292")
	gotAddr := stubListenAndServe(t, errors.New("stopped"))

	main()

	if *gotAddr != ":9292" {
		t.Fatalf("main() listened on %q, want %q", *gotAddr, ":9292")
	}
}
