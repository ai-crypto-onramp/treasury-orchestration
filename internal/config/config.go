// Package config loads Treasury Orchestration configuration from the
// environment. All values have sensible defaults so the service can boot
// without external dependencies (used by tests and the in-memory mode).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the top-level configuration for the service.
type Config struct {
	Port                  string
	DBURL                 string
	RedisURL              string
	BatchIntervalSeconds  int
	BatchSizeThresholdUSD float64
	MinFloatUSD           float64
	MaxFloatUSD           float64
	HotWalletTargets      map[string]float64
	LiquidityRoutingURL   string
	WalletMgmtURL         string
	FXHedgingURL          string
	LedgerURL             string
	AuditLogURL           string
	TxOrchEventTopic      string
	LogLevel              string

	// SettlementDays maps fiat currency -> T+n settlement days. Defaults
	// to T+2 for everything not listed.
	SettlementDays map[string]int

	// Per-asset-pair batch policy overrides. Keys are "asset_pair" e.g.
	// "BTC/USD". When absent, global defaults apply.
	BatchIntervalOverrides  map[string]int
	BatchThresholdOverrides map[string]float64
}

// Load reads configuration from the environment.
func Load() Config {
	cfg := Config{
		Port:                  envOr("PORT", "8080"),
		DBURL:                 os.Getenv("DB_URL"),
		RedisURL:              osOr("REDIS_URL", "redis://localhost:6379/0"),
		BatchIntervalSeconds:  envInt("BATCH_INTERVAL_SECONDS", 30),
		BatchSizeThresholdUSD: envFloat("BATCH_SIZE_THRESHOLD_USD", 50000),
		MinFloatUSD:           envFloat("MIN_FLOAT_USD", 250000),
		MaxFloatUSD:           envFloat("MAX_FLOAT_USD", 5000000),
		HotWalletTargets:      loadHotWalletTargets(),
		LiquidityRoutingURL:   os.Getenv("LIQUIDITY_ROUTING_URL"),
		WalletMgmtURL:         os.Getenv("WALLET_MGMT_URL"),
		FXHedgingURL:          os.Getenv("FX_HEDGING_URL"),
		LedgerURL:             os.Getenv("LEDGER_URL"),
		AuditLogURL:           os.Getenv("AUDIT_LOG_URL"),
		TxOrchEventTopic:      envOr("TX_ORCH_EVENT_TOPIC", "tx.completed"),
		LogLevel:              envOr("LOG_LEVEL", "info"),
		SettlementDays: map[string]int{
			"USD": envInt("SETTLEMENT_DAYS_USD", 2),
			"EUR": envInt("SETTLEMENT_DAYS_EUR", 2),
			"GBP": envInt("SETTLEMENT_DAYS_GBP", 2),
			"CHF": envInt("SETTLEMENT_DAYS_CHF", 2),
			"JPY": envInt("SETTLEMENT_DAYS_JPY", 3),
		},
		BatchIntervalOverrides:  loadIntOverrides("BATCH_INTERVAL_OVERRIDE_"),
		BatchThresholdOverrides: loadFloatOverrides("BATCH_THRESHOLD_OVERRIDE_"),
	}
	return cfg
}

// SettlementDaysFor returns the T+n settlement days for a fiat currency,
// defaulting to 2.
func (c Config) SettlementDaysFor(fiat string) int {
	if d, ok := c.SettlementDays[strings.ToUpper(fiat)]; ok && d > 0 {
		return d
	}
	return 2
}

// BatchIntervalFor returns the batch close cadence (seconds) for an asset
// pair, honoring per-pair overrides.
func (c Config) BatchIntervalFor(assetPair string) int {
	if v, ok := c.BatchIntervalOverrides[assetPair]; ok && v > 0 {
		return v
	}
	return cfgBatchIntervalDefault(&c)
}

func cfgBatchIntervalDefault(c *Config) int { return c.BatchIntervalSeconds }

// BatchThresholdFor returns the notional size threshold (USD) that triggers
// an early batch close for an asset pair, honoring per-pair overrides.
func (c Config) BatchThresholdFor(assetPair string) float64 {
	if v, ok := c.BatchThresholdOverrides[assetPair]; ok && v > 0 {
		return v
	}
	return c.BatchSizeThresholdUSD
}

// HotWalletTargetFor returns the target hot-wallet balance for an asset,
// defaulting to 0 when unset.
func (c Config) HotWalletTargetFor(asset string) float64 {
	if v, ok := c.HotWalletTargets[strings.ToUpper(asset)]; ok {
		return v
	}
	return 0
}

// BatchIntervalDuration returns the cadence as a time.Duration for an asset
// pair.
func (c Config) BatchIntervalDuration(assetPair string) time.Duration {
	return time.Duration(c.BatchIntervalFor(assetPair)) * time.Second
}

func loadHotWalletTargets() map[string]float64 {
	out := map[string]float64{}
	const prefix = "HOT_WALLET_TARGET_BALANCE_"
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k := kv[:eq]
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		asset := strings.ToUpper(strings.TrimPrefix(k, prefix))
		v, err := strconv.ParseFloat(kv[eq+1:], 64)
		if err != nil {
			continue
		}
		out[asset] = v
	}
	return out
}

func loadIntOverrides(prefix string) map[string]int {
	out := map[string]int{}
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k := kv[:eq]
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		name := strings.TrimPrefix(k, prefix)
		v, err := strconv.Atoi(kv[eq+1:])
		if err != nil {
			continue
		}
		out[name] = v
	}
	return out
}

func loadFloatOverrides(prefix string) map[string]float64 {
	out := map[string]float64{}
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k := kv[:eq]
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		name := strings.TrimPrefix(k, prefix)
		v, err := strconv.ParseFloat(kv[eq+1:], 64)
		if err != nil {
			continue
		}
		out[name] = v
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func osOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// ParseBool parses a boolean env value tolerant of common spellings.
func ParseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on", "y":
		return true
	}
	return false
}

// Unused guard to keep fmt import when not otherwise referenced.
var _ = fmt.Sprint