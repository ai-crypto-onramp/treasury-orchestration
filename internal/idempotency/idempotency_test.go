package idempotency

import (
	"context"
	"testing"
	"time"
)

func TestMemStore_CheckAndMark(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	ok, err := s.CheckAndMark(ctx, "k1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first: %v ok=%v", err, ok)
	}
	ok, _ = s.CheckAndMark(ctx, "k1", time.Minute)
	if ok {
		t.Fatal("expected dup false")
	}
	// Different key claims.
	ok, _ = s.CheckAndMark(ctx, "k2", time.Minute)
	if !ok {
		t.Fatal("expected k2 ok")
	}
}

func TestMemStore_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	ok, _ := s.CheckAndMark(ctx, "exp", 50*time.Millisecond)
	if !ok {
		t.Fatal("expected first ok")
	}
	time.Sleep(60 * time.Millisecond)
	ok, _ = s.CheckAndMark(ctx, "exp", 50*time.Millisecond)
	if !ok {
		t.Fatal("expected re-claim after expiry")
	}
}

func TestMemStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	_, _ = s.CheckAndMark(ctx, "d", time.Minute)
	if err := s.Delete(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	ok, _ := s.CheckAndMark(ctx, "d", time.Minute)
	if !ok {
		t.Fatal("expected re-claim after delete")
	}
}

func TestCadenceLock_Acquire(t *testing.T) {
	ctx := context.Background()
	lock := NewCadenceLock(NewMem(), time.Minute)
	ok, err := lock.Acquire(ctx, "BTC/USD")
	if err != nil || !ok {
		t.Fatalf("acquire: %v ok=%v", err, ok)
	}
	ok, _ = lock.Acquire(ctx, "BTC/USD")
	if ok {
		t.Fatal("expected second acquire to fail")
	}
	ok, _ = lock.Acquire(ctx, "ETH/USD")
	if !ok {
		t.Fatal("expected different pair to acquire")
	}
}

func TestOpen_EmptyURLReturnsMem(t *testing.T) {
	s, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*MemStore); !ok {
		t.Fatal("expected MemStore for empty url")
	}
}