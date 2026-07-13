// Command treasury is the entrypoint for the Treasury Orchestration
// service. It loads configuration, builds the wired server, and runs it.
//
// Run with `go run ./cmd/treasury` (local dev) or `make run`. See
// README.md for the full configuration surface.
package main

import (
	"log"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/app"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
)

func main() {
	cfg := config.Load()
	srv, err := app.Build(cfg)
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	if err := srv.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}