package db

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
)

// requiredTables are the six Stage-1 tables that must exist after Migrate.
var requiredTables = []string{
	"batches",
	"batch_memberships",
	"aggregate_orders",
	"funding_requests",
	"float_positions",
	"rebalancing_jobs",
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set; skipping Postgres-backed migration test")
	}
	cfg := Config{URL: url}
	ctx := context.Background()
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Clean slate: drop all known tables + tracking table so the test is
	// independent of prior runs.
	for _, tbl := range append(requiredTables, "schema_migrations") {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl+" CASCADE")
	}
	return db
}

func TestMigrate_UpCreatesAllTables(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	for _, tbl := range requiredTables {
		exists := tableExists(t, db, tbl)
		if !exists {
			t.Fatalf("table %q missing after Migrate", tbl)
		}
	}
}

func TestMigrate_IdempotentReapply(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	for _, tbl := range requiredTables {
		if !tableExists(t, db, tbl) {
			t.Fatalf("table %q missing after re-Migrate", tbl)
		}
	}
}

func TestRollback_DropsLastMigration(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := Rollback(ctx, db); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	// rebalancing_jobs is the last migration; should be gone after one rollback.
	if tableExists(t, db, "rebalancing_jobs") {
		t.Fatalf("rebalancing_jobs should be dropped after Rollback")
	}
	// Other tables must remain.
	for _, tbl := range requiredTables[:5] {
		if !tableExists(t, db, tbl) {
			t.Fatalf("table %q should remain after single Rollback", tbl)
		}
	}
}

func TestRollback_Idempotent(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	ctx := context.Background()

	// Rollback on a fresh DB (no migrations applied) is a no-op.
	if err := Rollback(ctx, db); err != nil {
		t.Fatalf("Rollback on empty DB: %v", err)
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var ok bool
	// name is a compile-time-known constant; safe to interpolate.
	q := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
	if err := db.QueryRowContext(context.Background(), q, name).Scan(&ok); err != nil {
		t.Fatalf("tableExists %s: %v", name, err)
	}
	return ok
}

func TestRequiredIndexesExist(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	want := []string{
		"idx_batches_asset_pair",
		"idx_batches_status",
		"idx_memberships_batch_id",
		"idx_float_positions_fiat_currency",
	}
	for _, idx := range want {
		if !indexExists(t, db, idx) {
			t.Fatalf("expected index %q to exist", idx)
		}
	}
}

func indexExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var ok bool
	q := "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)"
	if err := db.QueryRowContext(context.Background(), q, name).Scan(&ok); err != nil {
		t.Fatalf("indexExists %s: %v", name, err)
	}
	return ok
}

// guard against accidental SQL injection via table names in helpers.
func init() {
	for _, tbl := range append(requiredTables, "schema_migrations") {
		if !isIdentifier(tbl) {
			panic("invalid table identifier: " + tbl)
		}
	}
}

func isIdentifier(s string) bool {
	for _, r := range s {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return strings.TrimSpace(s) != ""
}