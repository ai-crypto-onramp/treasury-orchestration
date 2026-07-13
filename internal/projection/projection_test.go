package projection

import (
	"testing"
	"time"
)

func TestModel_ProjectedDemand(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", 1000, time.Now())
	m.Observe("BTC", 2000, time.Now())
	m.Observe("ETH", 500, time.Now())
	if got := m.ProjectedDemand("BTC"); got != 3000 {
		t.Fatalf("btc=%f want 3000", got)
	}
	if got := m.ProjectedDemand("ETH"); got != 500 {
		t.Fatalf("eth=%f want 500", got)
	}
	if got := m.ProjectedDemand("XRP"); got != 0 {
		t.Fatalf("xrp=%f want 0", got)
	}
}

func TestModel_EvictsOldSamples(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", 1000, time.Now().Add(-2*time.Minute))
	m.Observe("BTC", 2000, time.Now())
	if got := m.ProjectedDemand("BTC"); got != 2000 {
		t.Fatalf("btc=%f want 2000 (old evicted)", got)
	}
}

func TestModel_VelocityPerSecond(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", 6000, time.Now())
	v := m.VelocityPerSecond("BTC")
	if v <= 0 {
		t.Fatalf("velocity=%f want >0", v)
	}
	// 6000 / 60 = 100 per second.
	if v < 99 || v > 101 {
		t.Fatalf("velocity=%f want ~100", v)
	}
}