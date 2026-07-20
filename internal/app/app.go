// Package app is the composition root for the Treasury Orchestration
// service. It loads config, opens stores (in-memory by default, Postgres
// when DB_URL is set), constructs the consumer / scheduler / executor /
// float / funding / hedge / ledger subsystems, wires the REST handlers,
// and starts the HTTP server plus background loops.
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/aggregate"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/api"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/batch"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/consumer"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/eventbus"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/float"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/funding"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/hedge"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/ledger"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/projection"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/postgres"
)

// Server bundles the wired service.
type Server struct {
	cfg       config.Config
	http      *http.Server
	mux       http.Handler
	consumer  *consumer.Consumer
	scheduler *batch.Scheduler
	float     *float.Tracker
	emitter   *ledger.Emitter
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	db        *postgres.DB
}

// Build constructs the server from config. When DB_URL is empty it uses
// in-memory stores (only allowed in DEV_MODE=1); when set it opens Postgres
// and runs migrations.
func Build(cfg config.Config) (*Server, error) {
	// Apply essential defaults when the caller constructed Config
	// directly (without going through config.Load).
	if cfg.TxOrchEventTopic == "" {
		cfg.TxOrchEventTopic = "tx.completed"
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.BatchIntervalSeconds <= 0 {
		cfg.BatchIntervalSeconds = 30
	}
	if cfg.BatchSizeThresholdUSD <= 0 {
		cfg.BatchSizeThresholdUSD = 50000
	}

	ctx := context.Background()
	devMode := os.Getenv("DEV_MODE") == "1"
	if devMode {
		log.Printf("DEV_MODE=1: stub/fake clients in use — NOT FOR PRODUCTION")
	}

	var (
		batchStore    store.BatchStore
		membershipStore store.MembershipStore
		orderStore    store.AggregateOrderStore
		fundingStore  store.FundingStore
		floatStore    store.FloatStore
		rebalStore    store.RebalancingStore
		outboxStore   store.OutboxStore
		db            *postgres.DB
	)

	if cfg.DBURL != "" {
		var err error
		db, err = postgres.Open(ctx, cfg.DBURL)
		if err != nil {
			return nil, err
		}
		batchStore = db.Batch()
		membershipStore = db.Membership()
		orderStore = db.Order()
		fundingStore = db.Funding()
		floatStore = db.Float()
		rebalStore = db.Rebalance()
		outboxStore = db.Outbox()
	} else {
		if !devMode {
			return nil, errors.New("DB_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		all := memstore.NewAll()
		batchStore = all.Batch
		membershipStore = all.Membership
		orderStore = all.Order
		fundingStore = all.Funding
		floatStore = all.Float
		rebalStore = all.Rebalance
		outboxStore = all.Outbox
	}

	idem, err := idempotency.Open(ctx, cfg.RedisURL)
	if err != nil {
		return nil, err
	}
	cadence := idempotency.NewCadenceLock(idem, time.Duration(cfg.BatchIntervalSeconds)*time.Second)

	clients_, err := buildDownstreamClients(cfg, devMode)
	if err != nil {
		return nil, err
	}

	emitter := ledger.New(ledger.Deps{Outbox: outboxStore, Ledger: clients_.ledger, Audit: clients_.audit})

	proj := projection.New(5 * time.Minute)

	floatTracker := float.New(float.Deps{
		Cfg:    cfg,
		Floats: floatStore,
		OnAdjust: func(ctx context.Context, fiat string, amount float64, batchID uuid.UUID) {
			_ = emitter.Append(ctx, ledger.AggFloat, ledger.EvFloatAdjust, ledger.Key(ledger.AggFloat, ledger.EvFloatAdjust, batchID), ledger.Payload{
				BatchID:      batchID,
				NotionalUSD:  amount,
				FiatCurrency: fiat,
			})
		},
	})

	hedger := hedge.New(hedge.Deps{FX: clients_.fx, Orders: orderStore, Idem: idem})

	executor := aggregate.New(aggregate.Deps{
		Batches:          batchStore,
		Orders:           orderStore,
		Liquidity:        clients_.liquidity,
		Idem:             idem,
		ExpectedPriceFor: func(assetPair string) float64 { return 50000 },
		OnFill: func(ctx context.Context, b *store.Batch, o *store.AggregateOrder) {
			fiat := fiatOf(b.AssetPair)
			cryptoAsset := cryptoOf(b.AssetPair)
			_ = floatTracker.OnAggregateFill(ctx, b, o, fiat, cryptoAsset)
			_, _ = hedger.OnAggregateFill(ctx, b, o, fiat)
			_, _, _ = batchStore.UpdateBatchStatus(ctx, b.ID, store.BatchExecuting, store.BatchSettled, nil)
			_ = emitter.Append(ctx, ledger.AggAggregate, ledger.EvAggregateExec, ledger.Key(ledger.AggAggregate, ledger.EvAggregateExec, b.ID), ledger.Payload{
				BatchID:      b.ID,
				NotionalUSD:  o.NotionalUSD,
				Asset:        cryptoAsset,
				FiatCurrency: fiat,
			})
		},
	})

	scheduler := batch.New(batch.Deps{
		Cfg:         cfg,
		Batches:     batchStore,
		Memberships: membershipStore,
		Lock:        cadence,
		OnClose: func(ctx context.Context, b *store.Batch, reason batch.CloseReason) {
			_ = emitter.Append(ctx, ledger.AggBatch, ledger.EvBatchClose, ledger.Key(ledger.AggBatch, ledger.EvBatchClose, b.ID), ledger.Payload{
				BatchID:     b.ID,
				NotionalUSD: b.NotionalUSD,
			})
			_, _ = executor.SubmitBatch(ctx, b.ID)
		},
	})

	fundingMgr := funding.New(funding.Deps{
		Cfg:        cfg,
		Funding:    fundingStore,
		Rebalance:  rebalStore,
		Wallet:     clients_.wallet,
		Idem:       idem,
		Projection: proj,
		OnFunding: func(ctx context.Context, fr *store.FundingRequest) {
			_ = emitter.Append(ctx, ledger.AggFunding, ledger.EvFunding, ledger.Key(ledger.AggFunding, ledger.EvFunding, fr.ID), ledger.Payload{
				Asset:       fr.Asset,
				NotionalUSD: fr.Amount,
			})
		},
		OnRebalance: func(ctx context.Context, job *store.RebalancingJob) {
			_ = emitter.Append(ctx, ledger.AggRebalance, ledger.EvRebalance, ledger.Key(ledger.AggRebalance, ledger.EvRebalance, job.ID), ledger.Payload{
				Asset:       job.Asset,
				NotionalUSD: job.Amount,
			})
		},
	})

	httpPush := eventbus.NewHTTPPush()
	subscriber := eventbus.EventSubscriber(httpPush)
	if cfg.EventBusURL != "" {
		if ks, err := eventbus.NewKafkaSubscriberFromURL(cfg.EventBusURL, cfg.EventBusGroupID); err == nil {
			subscriber = ks
		} else {
			log.Printf("app: kafka subscriber init failed, using http-push: %v", err)
		}
	}

	cons := consumer.New(consumer.Deps{
		Topic:       cfg.TxOrchEventTopic,
		Batches:     batchStore,
		Memberships: membershipStore,
		Idem:        idem,
		Subscriber:  subscriber,
		OnBatchOpen: func(ctx context.Context, b *store.Batch) {
			_ = emitter.Append(ctx, ledger.AggBatch, ledger.EvBatchOpen, ledger.Key(ledger.AggBatch, ledger.EvBatchOpen, b.ID), ledger.Payload{
				BatchID: b.ID,
			})
		},
	})

	mux := api.NewRouter(&api.Deps{
		Batches:   batchStore,
		Members:   membershipStore,
		Orders:    orderStore,
		Scheduler: scheduler,
		Float:     floatTracker,
		Funding:   fundingMgr,
	})

	root := http.NewServeMux()
	root.Handle("/", mux)
	root.Handle("/v1/events/", httpPush.HTTPHandler())
	root.Handle("/metrics", promhttp.Handler())

	srv := &Server{
		cfg:       cfg,
		mux:       root,
		consumer:  cons,
		scheduler: scheduler,
		float:     floatTracker,
		emitter:   emitter,
		db:        db,
		http: &http.Server{
			Addr:              ":" + cfg.Port,
			Handler:           root,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	return srv, nil
}

// Run starts the HTTP server and the background loops and blocks until
// SIGINT/SIGTERM.
func (s *Server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	s.startLoops(ctx)
	log.Printf("treasury-orchestration listening on :%s", s.cfg.Port)
	errCh := make(chan error, 1)
	go func() { errCh <- s.http.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case <-sig:
		return s.Shutdown()
	}
}

func (s *Server) startLoops(ctx context.Context) {
	s.wg.Add(4)
	go func() { defer s.wg.Done(); _ = s.scheduler.Run(ctx) }()
	go func() { defer s.wg.Done(); _ = s.consumer.Run(ctx) }()
	go func() { defer s.wg.Done(); _ = s.emitter.RunDispatcherLoop(ctx, 5*time.Second) }()
	go func() { defer s.wg.Done(); _ = s.float.RunSweeperLoop(ctx, 30*time.Second) }()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if s.http != nil {
		err = s.http.Shutdown(ctx)
	}
	s.wg.Wait()
	if s.db != nil {
		_ = s.db.Close()
	}
	return err
}

// HTTPHandler returns the wired HTTP handler (test helper).
func (s *Server) HTTPHandler() http.Handler { return s.mux }

// ErrStopped is returned when the server has been stopped.
var ErrStopped = errors.New("app: stopped")

// fiatOf extracts the fiat side of an "ASSET/FIAT" pair, defaulting to
// USD.
func fiatOf(assetPair string) string {
	for i := 0; i < len(assetPair); i++ {
		if assetPair[i] == '/' {
			return assetPair[i+1:]
		}
	}
	return "USD"
}

// cryptoOf extracts the crypto side of an "ASSET/FIAT" pair.
func cryptoOf(assetPair string) string {
	for i := 0; i < len(assetPair); i++ {
		if assetPair[i] == '/' {
			return assetPair[:i]
		}
	}
	return assetPair
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

type downstreamClients struct {
	liquidity clients.LiquidityRouting
	fx        clients.FXHedging
	wallet    clients.WalletManagement
	ledger    clients.LedgerAccounting
	audit     clients.AuditLog
}

func buildDownstreamClients(cfg config.Config, devMode bool) (downstreamClients, error) {
	var dc downstreamClients
	liq, err := buildLiquidity(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.liquidity = liq
	fx, err := buildFX(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.fx = fx
	w, err := buildWallet(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.wallet = w
	lg, err := buildLedger(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.ledger = lg
	au, err := buildAudit(devMode)
	if err != nil {
		return dc, err
	}
	dc.audit = au
	return dc, nil
}

func buildLiquidity(cfg config.Config, devMode bool) (clients.LiquidityRouting, error) {
	if cfg.LiquidityRoutingURL != "" {
		return clients.NewResilientLiquidity(
			clients.NewHTTPLiquidity(cfg.LiquidityRoutingURL),
			clients.DefaultRetry(),
			clients.DefaultCircuitBreaker(),
		), nil
	}
	if !devMode {
		return nil, errors.New("LIQUIDITY_ROUTING_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeLiquidity(clients.FillResult{FillPrice: 50000, TotalFilled: 1}), nil
}

func buildFX(cfg config.Config, devMode bool) (clients.FXHedging, error) {
	if cfg.FXHedgingURL != "" {
		return clients.NewResilientFX(
			clients.NewHTTPFX(cfg.FXHedgingURL),
			clients.DefaultRetry(),
			clients.DefaultCircuitBreaker(),
		), nil
	}
	if !devMode {
		return nil, errors.New("FX_HEDGING_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeFX(clients.HedgeResult{HedgedNotional: 0}), nil
}

func buildWallet(cfg config.Config, devMode bool) (clients.WalletManagement, error) {
	if cfg.WalletMgmtURL != "" {
		return clients.NewHTTPWallet(cfg.WalletMgmtURL), nil
	}
	if !devMode {
		return nil, errors.New("WALLET_MGMT_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeWallet(clients.FundingMoveResult{Completed: true, TxID: "stub"}), nil
}

func buildLedger(cfg config.Config, devMode bool) (clients.LedgerAccounting, error) {
	if cfg.LedgerURL != "" {
		return clients.NewHTTPLedger(cfg.LedgerURL), nil
	}
	if !devMode {
		return nil, errors.New("LEDGER_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeLedger(), nil
}

func buildAudit(devMode bool) (clients.AuditLog, error) {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		kc, err := clients.NewKafkaAudit(splitCSV(brokers))
		if err != nil {
			if devMode {
				log.Printf("warn: audit kafka init failed (DEV_MODE): %v; using fake audit", err)
				return clients.NewFakeAudit(), nil
			}
			return nil, fmt.Errorf("audit kafka init: %w", err)
		}
		return kc, nil
	}
	if devMode {
		log.Printf("warn: KAFKA_BROKERS unset and DEV_MODE=1; audit events recorded in-memory only")
		return clients.NewFakeAudit(), nil
	}
	return nil, errors.New("KAFKA_BROKERS unset and DEV_MODE!=1; refusing to start in production mode")
}