package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var (
	upRe   = regexp.MustCompile(`(?ms)--\s*\+migrate\s+Up\s*\n(.*?)(?:--\s*\+migrate\s+Down|\z)`)
	downRe = regexp.MustCompile(`(?ms)--\s*\+migrate\s+Down\s*\n(.*?)\z`)
)

// Migration is a single numbered SQL migration parsed from the embedded FS.
type Migration struct {
	Version int
	Up      string
	Down    string
}

// LoadMigrations reads and parses embedded SQL migration files.
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	var ms []Migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ver, err := parseVersion(name)
		if err != nil {
			return nil, fmt.Errorf("parse version %q: %w", name, err)
		}
		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", name, err)
		}
		up := firstGroup(upRe, string(data))
		down := firstGroup(downRe, string(data))
		if up == "" {
			return nil, fmt.Errorf("migration %q missing +migrate Up block", name)
		}
		ms = append(ms, Migration{Version: ver, Up: up, Down: down})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	return ms, nil
}

func parseVersion(name string) (int, error) {
	// expects "<number>_<rest>.sql"
	num := regexp.MustCompile(`^(\d+)_`)
	m := num.FindStringSubmatch(name)
	if m == nil {
		return 0, fmt.Errorf("invalid migration filename")
	}
	return strconv.Atoi(m[1])
}

func firstGroup(re *regexp.Regexp, s string) string {
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// ensureSchemaMigrations creates the version-tracking table if missing.
const ensureSchemaMigrations = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// Migrate applies all pending up-migrations in order. It is idempotent: a
// migration already recorded in schema_migrations is skipped.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := runStmt(ctx, db, ensureSchemaMigrations); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	ms, err := LoadMigrations()
	if err != nil {
		return err
	}
	for _, m := range ms {
		applied, err := isApplied(ctx, db, m.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := applyUp(ctx, db, m); err != nil {
			return fmt.Errorf("apply migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// Rollback reverts the most recently applied migration. It is idempotent:
// if no migrations are applied or the down block is empty, it is a no-op.
func Rollback(ctx context.Context, db *sql.DB) error {
	if err := runStmt(ctx, db, ensureSchemaMigrations); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	ver, err := lastApplied(ctx, db)
	if err != nil {
		return err
	}
	if ver == 0 {
		return nil
	}
	ms, err := LoadMigrations()
	if err != nil {
		return err
	}
	var target Migration
	for _, m := range ms {
		if m.Version == ver {
			target = m
			break
		}
	}
	if target.Down == "" {
		return nil
	}
	if err := applyDown(ctx, db, target); err != nil {
		return fmt.Errorf("rollback migration %d: %w", target.Version, err)
	}
	return nil
}

func isApplied(ctx context.Context, db *sql.DB, ver int) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, ver,
	).Scan(&exists)
	return exists, err
}

func lastApplied(ctx context.Context, db *sql.DB) (int, error) {
	var ver sql.NullInt64
	err := db.QueryRowContext(ctx,
		`SELECT MAX(version) FROM schema_migrations`,
	).Scan(&ver)
	if err != nil {
		return 0, err
	}
	if !ver.Valid {
		return 0, nil
	}
	return int(ver.Int64), nil
}

func applyUp(ctx context.Context, db *sql.DB, m Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, m.Up); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func applyDown(ctx context.Context, db *sql.DB, m Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, m.Down); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM schema_migrations WHERE version=$1`, m.Version,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func runStmt(ctx context.Context, db *sql.DB, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
	return err
}