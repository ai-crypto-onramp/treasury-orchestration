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

// --- additional coverage ---

func TestLoad_EnvOverridesAndInvalidValues(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("REDIS_URL", "redis://example:6379/3")
	t.Setenv("BATCH_INTERVAL_SECONDS", "not-an-int")
	t.Setenv("BATCH_SIZE_THRESHOLD_USD", "not-a-float")
	t.Setenv("MIN_FLOAT_USD", "123.45")
	t.Setenv("MAX_FLOAT_USD", "678.90")
	t.Setenv("SETTLEMENT_DAYS_USD", "7")
	t.Setenv("TX_ORCH_EVENT_TOPIC", "custom.topic")
	t.Setenv("EVENT_BUS_GROUP_ID", "custom-group")
	t.Setenv("LOG_LEVEL", "debug")
	cfg := Load()
	if cfg.Port != "9090" {
		t.Fatalf("port=%s want 9090", cfg.Port)
	}
	if cfg.RedisURL != "redis://example:6379/3" {
		t.Fatalf("redis=%s want redis://example:6379/3", cfg.RedisURL)
	}
	// Invalid int falls back to default.
	if cfg.BatchIntervalSeconds != 30 {
		t.Fatalf("interval=%d want 30 (default on invalid)", cfg.BatchIntervalSeconds)
	}
	// Invalid float falls back to default.
	if cfg.BatchSizeThresholdUSD != 50000 {
		t.Fatalf("threshold=%f want 50000 (default on invalid)", cfg.BatchSizeThresholdUSD)
	}
	if cfg.MinFloatUSD != 123.45 {
		t.Fatalf("minfloat=%f want 123.45", cfg.MinFloatUSD)
	}
	if cfg.MaxFloatUSD != 678.90 {
		t.Fatalf("maxfloat=%f want 678.90", cfg.MaxFloatUSD)
	}
	if cfg.SettlementDaysFor("USD") != 7 {
		t.Fatalf("usd=%d want 7", cfg.SettlementDaysFor("USD"))
	}
	if cfg.TxOrchEventTopic != "custom.topic" {
		t.Fatalf("topic=%s want custom.topic", cfg.TxOrchEventTopic)
	}
	if cfg.EventBusGroupID != "custom-group" {
		t.Fatalf("group=%s want custom-group", cfg.EventBusGroupID)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("loglevel=%s want debug", cfg.LogLevel)
	}
}

func TestLoad_EnvIntAndFloatValidValues(t *testing.T) {
	t.Setenv("BATCH_INTERVAL_SECONDS", "42")
	t.Setenv("BATCH_SIZE_THRESHOLD_USD", "999.5")
	t.Setenv("SETTLEMENT_DAYS_JPY", "5")
	cfg := Load()
	if cfg.BatchIntervalSeconds != 42 {
		t.Fatalf("interval=%d want 42", cfg.BatchIntervalSeconds)
	}
	if cfg.BatchSizeThresholdUSD != 999.5 {
		t.Fatalf("threshold=%f want 999.5", cfg.BatchSizeThresholdUSD)
	}
	if cfg.SettlementDaysFor("JPY") != 5 {
		t.Fatalf("jpy=%d want 5", cfg.SettlementDaysFor("JPY"))
	}
}

func TestEnvOrAndEnvIntDirect(t *testing.T) {
	// These exercise the helper functions directly with env set.
	t.Setenv("PORT", "7070")
	t.Setenv("BATCH_INTERVAL_SECONDS", "11")
	if got := envOr("PORT", "fallback"); got != "7070" {
		t.Fatalf("envOr=%s want 7070", got)
	}
	if got := envInt("BATCH_INTERVAL_SECONDS", 99); got != 11 {
		t.Fatalf("envInt=%d want 11", got)
	}
	if got := envFloat("BATCH_SIZE_THRESHOLD_USD", 99.0); got != 99.0 {
		// Unset -> default.
		t.Fatalf("envFloat default=%f want 99", got)
	}
}

func TestLoadIntOverrides_InvalidValueSkipped(t *testing.T) {
	t.Setenv("BATCH_INTERVAL_OVERRIDE_BTC/USD", "not-an-int")
	t.Setenv("BATCH_INTERVAL_OVERRIDE_ETH/USD", "20")
	out := loadIntOverrides("BATCH_INTERVAL_OVERRIDE_")
	if _, ok := out["BTC/USD"]; ok {
		t.Fatal("expected invalid value to be skipped")
	}
	if out["ETH/USD"] != 20 {
		t.Fatalf("eth=%d want 20", out["ETH/USD"])
	}
}

func TestLoadFloatOverrides_InvalidValueSkipped(t *testing.T) {
	t.Setenv("BATCH_THRESHOLD_OVERRIDE_BTC/USD", "not-a-float")
	t.Setenv("BATCH_THRESHOLD_OVERRIDE_ETH/USD", "1234.5")
	out := loadFloatOverrides("BATCH_THRESHOLD_OVERRIDE_")
	if _, ok := out["BTC/USD"]; ok {
		t.Fatal("expected invalid float to be skipped")
	}
	if out["ETH/USD"] != 1234.5 {
		t.Fatalf("eth=%f want 1234.5", out["ETH/USD"])
	}
}

func TestLoadHotWalletTargets_InvalidValueSkipped(t *testing.T) {
	t.Setenv("HOT_WALLET_TARGET_BALANCE_BTC", "not-a-number")
	t.Setenv("HOT_WALLET_TARGET_BALANCE_ETH", "3.5")
	out := loadHotWalletTargets()
	if _, ok := out["BTC"]; ok {
		t.Fatal("expected invalid number to be skipped")
	}
	if out["ETH"] != 3.5 {
		t.Fatalf("eth=%f want 3.5", out["ETH"])
	}
}

func TestBatchIntervalAndThreshold_Defaults(t *testing.T) {
	cfg := Config{}
	if cfg.BatchIntervalFor("BTC/USD") != 0 {
		t.Fatalf("interval default=%d want 0", cfg.BatchIntervalFor("BTC/USD"))
	}
	if cfg.BatchThresholdFor("BTC/USD") != 0 {
		t.Fatalf("threshold default=%f want 0", cfg.BatchThresholdFor("BTC/USD"))
	}
	if cfg.BatchIntervalDuration("BTC/USD") != 0 {
		t.Fatalf("duration default=%v want 0", cfg.BatchIntervalDuration("BTC/USD"))
	}
}

func TestSettlementDaysFor_ZeroFallsBackToDefault(t *testing.T) {
	cfg := Config{SettlementDays: map[string]int{"USD": 0}}
	if got := cfg.SettlementDaysFor("USD"); got != 2 {
		t.Fatalf("usd=%d want 2 (zero falls back)", got)
	}
}

func TestHotWalletTargetFor_CaseInsensitive(t *testing.T) {
	cfg := Config{HotWalletTargets: map[string]float64{"BTC": 5}}
	if got := cfg.HotWalletTargetFor("btc"); got != 5 {
		t.Fatalf("btc=%f want 5", got)
	}
	if got := cfg.HotWalletTargetFor("ETH"); got != 0 {
		t.Fatalf("eth=%f want 0 (unset)", got)
	}
}