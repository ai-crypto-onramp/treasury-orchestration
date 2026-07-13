package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("DB_URL", "")
	t.Setenv("REDIS_URL", "")
	cfg := Load()
	if cfg.Port != "8080" {
		t.Fatalf("port=%s want 8080", cfg.Port)
	}
	if cfg.BatchIntervalSeconds != 30 {
		t.Fatalf("interval=%d want 30", cfg.BatchIntervalSeconds)
	}
	if cfg.BatchSizeThresholdUSD != 50000 {
		t.Fatalf("threshold=%f want 50000", cfg.BatchSizeThresholdUSD)
	}
	if cfg.TxOrchEventTopic != "tx.completed" {
		t.Fatalf("topic=%s want tx.completed", cfg.TxOrchEventTopic)
	}
	if cfg.SettlementDaysFor("JPY") != 3 {
		t.Fatalf("jpy=%d want 3", cfg.SettlementDaysFor("JPY"))
	}
	if cfg.SettlementDaysFor("USD") != 2 {
		t.Fatalf("usd=%d want 2", cfg.SettlementDaysFor("USD"))
	}
	if cfg.SettlementDaysFor("XXX") != 2 {
		t.Fatalf("xxx=%d want 2 (default)", cfg.SettlementDaysFor("XXX"))
	}
}

func TestLoad_HotWalletTargets(t *testing.T) {
	t.Setenv("HOT_WALLET_TARGET_BALANCE_USDC", "100000")
	t.Setenv("HOT_WALLET_TARGET_BALANCE_BTC", "5")
	cfg := Load()
	if cfg.HotWalletTargetFor("USDC") != 100000 {
		t.Fatalf("usdc=%f want 100000", cfg.HotWalletTargetFor("USDC"))
	}
	if cfg.HotWalletTargetFor("BTC") != 5 {
		t.Fatalf("btc=%f want 5", cfg.HotWalletTargetFor("BTC"))
	}
	if cfg.HotWalletTargetFor("ETH") != 0 {
		t.Fatalf("eth=%f want 0", cfg.HotWalletTargetFor("ETH"))
	}
}

func TestBatchIntervalOverrides(t *testing.T) {
	t.Setenv("BATCH_INTERVAL_OVERRIDE_BTC/USD", "10")
	t.Setenv("BATCH_THRESHOLD_OVERRIDE_ETH/USD", "25000")
	cfg := Load()
	if cfg.BatchIntervalFor("BTC/USD") != 10 {
		t.Fatalf("override interval=%d want 10", cfg.BatchIntervalFor("BTC/USD"))
	}
	if cfg.BatchIntervalFor("BTC/USD") == cfg.BatchIntervalSeconds {
		t.Fatal("expected override to take effect")
	}
	if cfg.BatchThresholdFor("ETH/USD") != 25000 {
		t.Fatalf("override threshold=%f want 25000", cfg.BatchThresholdFor("ETH/USD"))
	}
	if cfg.BatchIntervalDuration("BTC/USD") != 10*time.Second {
		t.Fatalf("duration=%v want 10s", cfg.BatchIntervalDuration("BTC/USD"))
	}
}

func TestParseBool(t *testing.T) {
	for _, s := range []string{"1", "true", "yes", "on", "Y", "TRUE"} {
		if !ParseBool(s) {
			t.Fatalf("ParseBool(%q) want true", s)
		}
	}
	for _, s := range []string{"0", "false", "no", "", "x"} {
		if ParseBool(s) {
			t.Fatalf("ParseBool(%q) want false", s)
		}
	}
}