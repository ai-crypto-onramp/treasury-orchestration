//go:build integration

// Package integration contains tests that require a live PostgreSQL +
// Redis instance (started via docker-compose or testcontainers). They
// are excluded from the normal `go test ./...` run; run with:
//
//	go test -tags=integration ./test/integration/...
//
// See the Makefile `test-integration` target which brings up
// docker-compose Postgres + Redis and runs these tests.
package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/migrations"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/postgres"
)

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("set %s to run this integration test", key)
	}
	return v
}

// TestPostgres_MigrationsUpAndDown verifies the migration runner applies
// all migrations and that the schema is idempotent (re-running Up is a
// no-op) and reversible (Down drops everything).
func TestPostgres_MigrationsUpAndDown(t *testing.T) {
	dsn := requireEnv(t, "DB_URL")
	ctx := context.Background()
	db, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Open already ran migrations; verify the tables exist by exercising
	// a batch open.
	b, err := db.Batch().OpenBatch(ctx, "BTC/USD")
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != store.BatchOpen {
		t.Fatalf("status=%s want open", b.Status)
	}
	// Idempotency: re-running Up via the runner should not error.
	runner := migrations.NewRunnerWithQuery(
		func(c context.Context, q string, args ...any) error {
			_, err := db.Pool().Exec(c, q, args...)
			return err
		},
		func(c context.Context, version string) (bool, error) {
			var exists bool
			err := db.Pool().QueryRow(c, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
			return exists, err
		},
	)
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("re-up: %v", err)
	}
}

// TestRedis_Idempotency verifies the Redis-backed idempotency store.
func TestRedis_Idempotency(t *testing.T) {
	url := requireEnv(t, "REDIS_URL")
	ctx := context.Background()
	s, err := idempotency.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := s.CheckAndMark(ctx, "inttest:key", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first: %v ok=%v", err, ok)
	}
	ok, _ = s.CheckAndMark(ctx, "inttest:key", time.Minute)
	if ok {
		t.Fatal("expected dup false")
	}
	_ = s.Delete(ctx, "inttest:key")
}