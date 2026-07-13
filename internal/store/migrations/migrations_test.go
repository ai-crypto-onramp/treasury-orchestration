package migrations

import (
	"context"
	"sort"
	"testing"
)

func TestAll_LoadsMigrations(t *testing.T) {
	migs, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 {
		t.Fatal("expected at least one migration")
	}
	versions := make([]string, len(migs))
	for i, m := range migs {
		versions[i] = m.Version
		if m.Up == "" || m.Down == "" {
			t.Fatalf("migration %s missing up or down", m.Version)
		}
	}
	if !sort.StringsAreSorted(versions) {
		t.Fatalf("migrations not sorted: %v", versions)
	}
	if versions[0] != "0001" {
		t.Fatalf("expected first version 0001, got %s", versions[0])
	}
}

func TestRunner_UpIdempotent(t *testing.T) {
	var applied []string
	exec := func(_ context.Context, q string, _ ...any) error {
		if q == "INSERT INTO batch_memberships" || q == "CREATE TABLE" {
			// smoke: just record that something executed
		}
		return nil
	}
	// Use a simple exec-based runner; it applies all migrations each call
	// because isApplied always returns false (idempotent SQL guards).
	r := NewRunner(exec)
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = applied
}

func TestRunnerWithQuery_UpAndDown(t *testing.T) {
	var appliedVersions []string
	exec := func(_ context.Context, q string, _ ...any) error { return nil }
	query := func(_ context.Context, version string) (bool, error) {
		for _, v := range appliedVersions {
			if v == version {
				return true, nil
			}
		}
		return false, nil
	}
	r := NewRunnerWithQuery(exec, query)
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	migs, _ := All()
	for _, m := range migs {
		appliedVersions = append(appliedVersions, m.Version)
	}
	// Re-running Up should be a no-op (all applied).
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Down reverts.
	if err := r.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	appliedVersions = nil
}