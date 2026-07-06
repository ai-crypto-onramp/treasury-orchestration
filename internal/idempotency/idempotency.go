// Package idempotency provides a Redis-backed key store for write-side
// replay deduplication. If REDIS_URL is not set, the store is a no-op
// (always reports "not seen"), so local development does not require Redis.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrRedisUnavailable is returned when Redis is configured but unreachable.
var ErrRedisUnavailable = errors.New("redis unavailable")

// Store deduplicates write operations by key.
type Store struct {
	c       *redis.Client
	enabled bool
}

// ConfigFromEnv builds a Store from REDIS_URL. If empty, returns a disabled
// (no-op) store.
func ConfigFromEnv() *Store {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		return &Store{enabled: false}
	}
	opt, err := redis.ParseURL(addr)
	if err != nil {
		return &Store{enabled: false}
	}
	return &Store{c: redis.NewClient(opt), enabled: true}
}

// Ping verifies Redis connectivity. Returns nil if the store is disabled.
func (s *Store) Ping(ctx context.Context) error {
	if !s.enabled {
		return nil
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.c.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
	}
	return nil
}

// CheckAndMark returns true if the key was already recorded (a replay).
// On first sight it records the key with the given TTL and returns false.
// A disabled store always returns false (not seen) and stores nothing.
func (s *Store) CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if !s.enabled {
		return false, nil
	}
	ok, err := s.c.SetNX(ctx, "idem:"+key, 1, ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// Close releases the Redis connection if enabled.
func (s *Store) Close() error {
	if !s.enabled || s.c == nil {
		return nil
	}
	return s.c.Close()
}