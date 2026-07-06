package db

import "testing"

func TestLoadMigrations_OrderedAndComplete(t *testing.T) {
	ms, err := LoadMigrations()
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}
	if len(ms) != 6 {
		t.Fatalf("expected 6 migrations, got %d", len(ms))
	}
	for i, m := range ms {
		if m.Version != i+1 {
			t.Fatalf("migration %d has version %d, expected %d", i, m.Version, i+1)
		}
		if m.Up == "" {
			t.Fatalf("migration %d has empty Up block", m.Version)
		}
	}
}

func TestLoadMigrations_AllTablesCovered(t *testing.T) {
	ms, _ := LoadMigrations()
	want := map[int]string{
		1: "batches",
		2: "batch_memberships",
		3: "aggregate_orders",
		4: "funding_requests",
		5: "float_positions",
		6: "rebalancing_jobs",
	}
	got := map[int]bool{}
	for _, m := range ms {
		for ver, table := range want {
			if m.Version == ver && contains(m.Up, "CREATE TABLE") && contains(m.Up, table) {
				got[ver] = true
			}
		}
	}
	for ver, table := range want {
		if !got[ver] {
			t.Fatalf("migration for table %q (version %d) not found or missing CREATE TABLE", table, ver)
		}
	}
}

func TestLoadMigrations_IndexesOnRequiredColumns(t *testing.T) {
	ms, _ := LoadMigrations()
	all := ""
	for _, m := range ms {
		all += m.Up + "\n"
	}
	for _, col := range []string{"batch_id", "status", "asset_pair", "fiat_currency"} {
		if !contains(all, "idx_") || !contains(all, col) {
			t.Fatalf("expected an index referencing column %q", col)
		}
	}
}

func TestLoadMigrations_DownBlocksPresent(t *testing.T) {
	ms, _ := LoadMigrations()
	for _, m := range ms {
		if m.Down == "" {
			t.Fatalf("migration %d missing +migrate Down block", m.Version)
		}
		if !contains(m.Down, "DROP TABLE") {
			t.Fatalf("migration %d Down block missing DROP TABLE", m.Version)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}