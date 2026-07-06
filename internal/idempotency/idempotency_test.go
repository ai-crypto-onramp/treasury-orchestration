package idempotency

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDisabledStore_NoOp(t *testing.T) {
	s := &Store{enabled: false}
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("disabled Ping should be nil, got %v", err)
	}
	seen, err := s.CheckAndMark(context.Background(), "k", time.Minute)
	if err != nil {
		t.Fatalf("disabled CheckAndMark err: %v", err)
	}
	if seen {
		t.Fatalf("disabled store should never report seen")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("disabled Close err: %v", err)
	}
}

func TestConfigFromEnv_DisabledWhenEmpty(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	s := ConfigFromEnv()
	if s.enabled {
		t.Fatalf("expected disabled store when REDIS_URL empty")
	}
}

func TestConfigFromEnv_DisabledOnInvalidURL(t *testing.T) {
	t.Setenv("REDIS_URL", "://bad")
	s := ConfigFromEnv()
	if s.enabled {
		t.Fatalf("expected disabled store on invalid REDIS_URL")
	}
	_ = os.Unsetenv("REDIS_URL")
}