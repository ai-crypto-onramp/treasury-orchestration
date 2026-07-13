package funding

import (
	"context"
	"testing"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/config"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/idempotency"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/projection"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func newMgr(t *testing.T) (*Manager, *memstore.All, *clients.FakeWallet, *projection.Model) {
	t.Helper()
	all := memstore.NewAll()
	wallet := clients.NewFakeWallet(clients.FundingMoveResult{Completed: true, TxID: "wtx"})
	proj := projection.New(60 * 1000_000_000) // 1m
	mgr := New(Deps{
		Cfg:        config.Config{HotWalletTargets: map[string]float64{"BTC": 10}},
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     wallet,
		Idem:       idempotency.NewMem(),
		Projection: proj,
	})
	return mgr, all, wallet, proj
}

func TestManager_CreateFundingRequest_PersistsAndExecutes(t *testing.T) {
	ctx := context.Background()
	mgr, all, wallet, _ := newMgr(t)
	fr, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", 5, "venueA")
	if err != nil {
		t.Fatal(err)
	}
	if fr.Status != "completed" {
		t.Fatalf("status=%s want completed", fr.Status)
	}
	calls := wallet.Calls()
	if len(calls) != 1 {
		t.Fatalf("wallet calls=%d want 1", len(calls))
	}
	if calls[0].WalletID != "hot1" || calls[0].Asset != "BTC" || calls[0].Amount != 5 {
		t.Fatalf("call=%+v", calls[0])
	}
	got, _ := all.Funding.GetFunding(ctx, fr.ID)
	if got.Status != "completed" {
		t.Fatalf("persisted status=%s want completed", got.Status)
	}
}

func TestManager_CreateFundingRequest_RejectsInvalidAmount(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", 0, ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", -1, ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
}

func TestManager_CreateFundingRequest_RejectsPolicyViolation(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", MaxFundingAmount+1, ""); err != ErrPolicyViolation {
		t.Fatalf("err=%v want ErrPolicyViolation", err)
	}
}

func TestManager_Rebalance_PersistsAndExecutes(t *testing.T) {
	ctx := context.Background()
	mgr, all, wallet, _ := newMgr(t)
	job, err := mgr.Rebalance(ctx, "venueA", "hot1", "BTC", 2, "drift")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "completed" {
		t.Fatalf("status=%s want completed", job.Status)
	}
	calls := wallet.Calls()
	if len(calls) != 1 {
		t.Fatalf("wallet calls=%d want 1", len(calls))
	}
	jobs, _ := all.Rebalance.ListJobs(ctx, "")
	if len(jobs) != 1 {
		t.Fatalf("persisted jobs=%d want 1", len(jobs))
	}
}

func TestManager_Rebalance_RejectsPolicyViolation(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", MaxFundingAmount+1, ""); err != ErrPolicyViolation {
		t.Fatalf("err=%v want ErrPolicyViolation", err)
	}
}

func TestManager_ListFunding_StatusFilter(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	_, _ = mgr.CreateFundingRequest(ctx, "h1", "BTC", 1, "")
	_, _ = mgr.CreateFundingRequest(ctx, "h2", "ETH", 1, "")
	all, _ := mgr.ListFunding(ctx, "")
	if len(all) != 2 {
		t.Fatalf("all=%d want 2", len(all))
	}
	completed, _ := mgr.ListFunding(ctx, "completed")
	if len(completed) != 2 {
		t.Fatalf("completed=%d want 2", len(completed))
	}
	pending, _ := mgr.ListFunding(ctx, "pending")
	if len(pending) != 0 {
		t.Fatalf("pending=%d want 0", len(pending))
	}
}

func TestManager_ListJobs_StatusFilter(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	_, _ = mgr.Rebalance(ctx, "a", "b", "BTC", 1, "x")
	_, _ = mgr.Rebalance(ctx, "c", "d", "ETH", 2, "y")
	all, _ := mgr.ListJobs(ctx, "")
	if len(all) != 2 {
		t.Fatalf("all=%d want 2", len(all))
	}
}