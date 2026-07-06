package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Config holds Postgres connection configuration.
type Config struct {
	URL string
}

// ConfigFromEnv reads DB_URL from the environment.
func ConfigFromEnv() Config {
	return Config{URL: os.Getenv("DB_URL")}
}

// Open creates a *sql.DB pool backed by pgx, configured for the treasury
// workload. If cfg.URL is empty it returns an error.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("DB_URL is required")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DB_URL: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("DB_URL must use postgres:// scheme, got %q", u.Scheme)
	}
	connCfg, err := pgx.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse pgx config: %w", err)
	}
	db := stdlib.OpenDB(*connCfg)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
}