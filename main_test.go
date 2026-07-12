package main

import (
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
