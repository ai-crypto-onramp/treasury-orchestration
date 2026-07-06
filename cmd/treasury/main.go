package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/db"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
)

// ready is set after migrations complete and dependencies are reachable.
var ready atomic.Bool

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.New(os.Stdout, "treasury ", log.LstdFlags|log.Lmsgprefix)

	// Postgres pool + migration runner.
	var pgErr error
	dbCfg := db.ConfigFromEnv()
	var pgPoolCloser func()
	if dbCfg.URL != "" {
		pool, err := db.Open(ctx, dbCfg)
		if err != nil {
			pgErr = err
			logger.Printf("db open failed: %v", err)
		} else {
			pgPoolCloser = func() { pool.Close() }
			if err := db.Migrate(ctx, pool); err != nil {
				pgErr = err
				logger.Printf("migrate failed: %v", err)
			}
		}
	} else {
		logger.Printf("DB_URL not set; skipping Postgres bootstrap")
	}

	// Redis idempotency store.
	idem := idempotency.ConfigFromEnv()
	if err := idem.Ping(ctx); err != nil {
		logger.Printf("redis ping failed: %v", err)
	} else if os.Getenv("REDIS_URL") != "" {
		logger.Printf("redis idempotency store ready")
	}
	defer idem.Close()

	if pgErr == nil && dbCfg.URL != "" {
		ready.Store(true)
		logger.Printf("treasury ready")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Printf("listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Printf("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	if pgPoolCloser != nil {
		pgPoolCloser()
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// readyHandler returns 200 once migrations have run and dependencies are
// reachable. Returns 503 otherwise, so orchestrators wait for a clean boot.
func readyHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}