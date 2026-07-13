package main

import (
	"encoding/json"
	"net/http"
	"os"
)

// listenAndServe is a package-level variable so tests can stub out the
// blocking server startup without changing runtime behavior.
var listenAndServe = http.ListenAndServe

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// readinessCheck describes a single readiness probe.
type readinessCheck struct {
	name string
	fn   func() bool
}

// dbReady is a stub for a database ping; in production this would
// perform an actual round-trip to the ledger store.
func dbReady() bool { return true }

// mqReady is a stub for the message-broker heartbeat.
func mqReady() bool { return true }

// signerReady is a stub for the MPC signing service heartbeat.
func signerReady() bool { return true }

// pricingReady is a stub for the pricing-quote service heartbeat.
func pricingReady() bool { return true }

// liquidityReady is a stub for the liquidity-routing service heartbeat.
func liquidityReady() bool { return true }

// railReady is a stub for the rail-connectors service heartbeat.
func railReady() bool { return true }

// fxReady is a stub for the fx-hedging service heartbeat.
func fxReady() bool { return true }

// blockchainReady is a stub for the blockchain-gateway heartbeat.
func blockchainReady() bool { return true }

// amlReady is a stub for the aml-kyt-screening service heartbeat.
func amlReady() bool { return true }

// identityReady is a stub for the identity-auth service heartbeat.
func identityReady() bool { return true }

// onboardingReady is a stub for the onboarding-kyc service heartbeat.
func onboardingReady() bool { return true }

// walletReady is a stub for the wallet-management service heartbeat.
func walletReady() bool { return true }

// notificationReady is a stub for the notification service heartbeat.
func notificationReady() bool { return true }

// policyReady is a stub for the policy-risk-engine service heartbeat.
func policyReady() bool { return true }

// auditReady is a stub for the audit-event-log service heartbeat.
func auditReady() bool { return true }

// ledgerReady is a stub for the ledger-accounting service heartbeat.
func ledgerReady() bool { return true }

// exchangeReady is a stub for the exchange-connectors service heartbeat.
func exchangeReady() bool { return true }

// mpcReady is a stub for the mpc-signing-service heartbeat.
func mpcReady() bool { return true }

// readinessChecks returns the ordered list of dependency probes. It
// is a variable so tests can substitute a smaller, controllable set.
var readinessChecks = func() []readinessCheck {
	return []readinessCheck{
		{name: "db", fn: dbReady},
		{name: "mq", fn: mqReady},
		{name: "signer", fn: signerReady},
		{name: "pricing", fn: pricingReady},
		{name: "liquidity", fn: liquidityReady},
		{name: "rail", fn: railReady},
		{name: "fx", fn: fxReady},
		{name: "blockchain", fn: blockchainReady},
		{name: "aml", fn: amlReady},
		{name: "identity", fn: identityReady},
		{name: "onboarding", fn: onboardingReady},
		{name: "wallet", fn: walletReady},
		{name: "notification", fn: notificationReady},
		{name: "policy", fn: policyReady},
		{name: "audit", fn: auditReady},
		{name: "ledger", fn: ledgerReady},
		{name: "exchange", fn: exchangeReady},
		{name: "mpc", fn: mpcReady},
	}
}

// readinessReport runs every probe and aggregates the results into a
// map suitable for JSON serialization, along with summary counts.
func readinessReport() (map[string]string, int, int) {
	results := map[string]string{}
	failed := 0
	total := 0
	for _, c := range readinessChecks() {
		total++
		if c.fn() {
			results[c.name] = "ok"
		} else {
			results[c.name] = "down"
			failed++
		}
	}
	return results, failed, total
}

// classifyReadiness maps the aggregate counts to an HTTP status code
// and a human-readable status string.
func classifyReadiness(failed, total int) (int, string) {
	if failed == total && total > 0 {
		return http.StatusServiceUnavailable, "not ready"
	}
	if failed > 0 {
		return http.StatusOK, "degraded"
	}
	return http.StatusOK, "ready"
}

// readyHandler reports readiness. It returns 503 only when every
// dependency probe fails simultaneously; otherwise it returns 200
// with the per-check breakdown.
func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	results, failed, total := readinessReport()
	code, status := classifyReadiness(failed, total)
	w.WriteHeader(code)
	results["status"] = status
	results["healthy"] = itoa(total - failed)
	results["failed"] = itoa(failed)
	results["total"] = itoa(total)
	_ = json.NewEncoder(w).Encode(results)
}

// itoa converts a small non-negative integer to its decimal string
// representation without pulling in strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// newMux builds the HTTP routing table for the service.
func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)
	return mux
}

// resolvePort returns the port from the PORT environment variable,
// defaulting to 8080 when unset.
func resolvePort() string {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return port
}

// run wires up the server and starts it, returning any startup error.
func run() error {
	return listenAndServe(":"+resolvePort(), newMux())
}

func main() {
	_ = run()
}
