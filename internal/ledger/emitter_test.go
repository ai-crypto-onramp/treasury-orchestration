package ledger

import (
	"context"
	"testing"

	"github.com/ai-crypto-onramp/treasury-orchestration/internal/clients"
	"github.com/ai-crypto-onramp/treasury-orchestration/internal/store/memstore"
)

func TestEmitter_AppendAndDispatch(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	ledgerCli := clients.NewFakeLedger()
	auditCli := clients.NewFakeAudit()
	e := New(Deps{Outbox: all.Outbox, Ledger: ledgerCli, Audit: auditCli})

	if err := e.Append(ctx, AggBatch, EvBatchClose, "batch.close:1", Payload{BatchID: 1, NotionalUSD: 50000}); err != nil {
		t.Fatal(err)
	}
	n, err := e.Dispatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dispatched=%d want 1", n)
	}
	ledgerCalls := ledgerCli.Calls()
	if len(ledgerCalls) != 1 {
		t.Fatalf("ledger calls=%d want 1", len(ledgerCalls))
	}
	if ledgerCalls[0].EventType != "batch.close" {
		t.Fatalf("event_type=%s want batch.close", ledgerCalls[0].EventType)
	}
	auditCalls := auditCli.Calls()
	if len(auditCalls) != 1 {
		t.Fatalf("audit calls=%d want 1", len(auditCalls))
	}
	if auditCalls[0].EventType != "batch.close" {
		t.Fatalf("event_type=%s want batch.close", auditCalls[0].EventType)
	}
	// Outbox entry marked emitted.
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("pending=%d want 0", len(pending))
	}
}

func TestEmitter_DedupByKey(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	e := New(Deps{Outbox: all.Outbox, Ledger: clients.NewFakeLedger(), Audit: clients.NewFakeAudit()})
	if err := e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: 1}); err != nil {
		t.Fatal(err)
	}
	// Same dedup key -> outbox Append returns false, no new entry.
	if err := e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: 1}); err != nil {
		t.Fatal(err)
	}
	snap := all.Outbox.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(snap))
	}
}

func TestEmitter_DispatchRetriesOnLedgerError(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	ledgerCli := clients.NewFakeLedger()
	ledgerCli.SetError(clients.ErrUnavailable)
	e := New(Deps{Outbox: all.Outbox, Ledger: ledgerCli, Audit: clients.NewFakeAudit()})
	_ = e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: 1})
	n, _ := e.Dispatch(ctx, 10)
	if n != 0 {
		t.Fatalf("dispatched=%d want 0 (ledger failed)", n)
	}
	// Entry remains pending.
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("pending=%d want 1 (retry)", len(pending))
	}
	// Clear the error and dispatch again.
	ledgerCli.SetError(nil)
	n2, _ := e.Dispatch(ctx, 10)
	if n2 != 1 {
		t.Fatalf("dispatched=%d want 1 on retry", n2)
	}
}

func TestEmitter_Key(t *testing.T) {
	if got := Key(AggBatch, EvBatchClose, 42); got != "batch.batch.close:42" {
		t.Fatalf("key=%q want batch.batch.close:42", got)
	}
}