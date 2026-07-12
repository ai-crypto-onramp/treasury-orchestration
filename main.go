package main

import (
	"encoding/json"
	"net/http"
	"os"
)

// listenAndServe is a package-level variable so tests can stub out the
// blocking server startup without changing runtime behavior.
var listenAndServe = http.ListenAndServe

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// newMux builds the HTTP routing table for the service.
func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	return mux
}

// resolvePort returns the port from the PORT environment variable,
// defaulting to 8080 when unset.
func resolvePort() string {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return port
}

// run wires up the server and starts it, returning any startup error.
func run() error {
	return listenAndServe(":"+resolvePort(), newMux())
}

func main() {
	_ = run()
}
