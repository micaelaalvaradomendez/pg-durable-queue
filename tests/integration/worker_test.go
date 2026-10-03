package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/fakeupstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
)

func fakeupstreamOK() fakeupstream.Config { return fakeupstream.Config{} }

func TestConcurrentWorkersNeverProcessTheSameItem(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 200, orderRef)
	fake, client := newFake(t, fakeupstreamOK())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 4 {
		w := newWorker(st, client, fastConfig("w-"+string(rune('a'+i))))
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(ctx) }()
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		p, err := st.GetProgress(context.Background(), b.ID)
		if err != nil {
			t.Fatal(err)
		}
		if p.Done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not drained: %v", p.Counts)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	items, err := st.ListItems(context.Background(), b.ID, "", 200, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.Status != batch.StatusApplied {
			t.Fatalf("%s: status %s", it.OrderRef, it.Status)
		}
		if n := fake.Requests(it.ID.String()); n != 1 {
			t.Fatalf("%s was sent upstream %d times, want exactly 1", it.OrderRef, n)
		}
	}
}

func TestDroppedResponseAfterApplyDoesNotDuplicate(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 30, orderRef)
	fake, client := newFake(t, fakeupstream.Config{DropAfterApplyFirst: true})

	p := drain(t, st, b.ID, newWorker(st, client, fastConfig("w-1")))
	if p.Counts[batch.StatusApplied] != 30 {
		t.Fatalf("applied = %d, want 30", p.Counts[batch.StatusApplied])
	}
	items, _ := st.ListItems(context.Background(), b.ID, "", 30, 0)
	for _, it := range items {
		if n := fake.Applications(it.OrderRef); n != 1 {
			t.Fatalf("%s applied %d times upstream, want exactly 1", it.OrderRef, n)
		}
	}
	// Every lost response was resolved by the status lookup, not by blindly resending.
	if n := count(t, st, `SELECT count(*) FROM item_attempt WHERE outcome = 'already_applied'`); n != 30 {
		t.Fatalf("already_applied attempts = %d, want 30", n)
	}
	if fake.Snapshot().Duplicates != 0 {
		t.Fatal("duplicates detected upstream")
	}
}

func TestTransientFailuresAreRetriedWithBackoff(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 10, orderRef)
	_, client := newFake(t, fakeupstream.Config{FailFirstN: 2})

	p := drain(t, st, b.ID, newWorker(st, client, fastConfig("w-1")))
	if p.Counts[batch.StatusApplied] != 10 {
		t.Fatalf("applied = %d, want 10 (%v)", p.Counts[batch.StatusApplied], p.Counts)
	}
	if n := count(t, st, `SELECT count(*) FROM item_attempt WHERE outcome = 'transient'`); n != 20 {
		t.Fatalf("transient attempts = %d, want 20 (2 per item)", n)
	}
	if n := count(t, st, `SELECT count(*) FROM batch_item WHERE attempts <> 3`); n != 0 {
		t.Fatalf("%d items did not take exactly 3 attempts", n)
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 3, orderRef)
	_, client := newFake(t, fakeupstream.Config{FailAlways: true})

	p := drain(t, st, b.ID, newWorker(st, client, fastConfig("w-1")))
	if p.Counts[batch.StatusDLQ] != 3 {
		t.Fatalf("dlq = %d, want 3", p.Counts[batch.StatusDLQ])
	}
	if n := count(t, st, `SELECT count(*) FROM item_attempt`); n != 3*4 {
		t.Fatalf("attempts = %d, want %d (MaxAttempts per item)", n, 3*4)
	}
	if n := count(t, st, `SELECT count(*) FROM outbox WHERE event_type = 'item.dead_lettered'`); n != 3 {
		t.Fatalf("dead-letter events = %d, want 3", n)
	}
}

func TestPermanentErrorsGoStraightToDLQ(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 4, func(i int) string {
		if i == 0 {
			return "INVALID-0001"
		}
		return orderRef(i)
	})
	_, client := newFake(t, fakeupstreamOK())

	p := drain(t, st, b.ID, newWorker(st, client, fastConfig("w-1")))
	if p.Counts[batch.StatusDLQ] != 1 || p.Counts[batch.StatusApplied] != 3 {
		t.Fatalf("counts = %v, want 1 dlq and 3 applied", p.Counts)
	}
	if n := count(t, st, `SELECT attempts FROM batch_item WHERE order_ref = 'INVALID-0001'`); n != 1 {
		t.Fatalf("permanent error retried: attempts = %d", n)
	}
}

func TestExpiredLeaseIsReclaimedAndStaleResultDiscarded(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	b := newBatch(t, st, 5, orderRef)

	// A worker claims the items and "crashes" before reporting back.
	crashed, err := st.ClaimItems(ctx, 10, 50*time.Millisecond)
	if err != nil || len(crashed) != 5 {
		t.Fatalf("claim: n=%d err=%v", len(crashed), err)
	}
	time.Sleep(100 * time.Millisecond)

	_, client := newFake(t, fakeupstreamOK())
	p := drain(t, st, b.ID, newWorker(st, client, fastConfig("w-2")))
	if p.Counts[batch.StatusApplied] != 5 {
		t.Fatalf("applied = %d, want 5", p.Counts[batch.StatusApplied])
	}

	// The crashed worker wakes up and reports a stale result: it must be ignored.
	ok, err := st.Complete(ctx, store.Result{
		Item: crashed[0], WorkerID: "w-1", Outcome: batch.OutcomePermanent,
		Next: batch.StatusDLQ, Detail: "stale",
	})
	if err != nil || ok {
		t.Fatalf("stale completion must be rejected: ok=%v err=%v", ok, err)
	}
	if n := count(t, st, `SELECT count(*) FROM batch_item WHERE status = 'dlq'`); n != 0 {
		t.Fatal("stale result overwrote the applied item")
	}
}

func TestRequeueFromDLQ(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	b := newBatch(t, st, 2, orderRef)
	_, failing := newFake(t, fakeupstream.Config{FailAlways: true})
	drain(t, st, b.ID, newWorker(st, failing, fastConfig("w-1")))

	items, _ := st.ListItems(ctx, b.ID, batch.StatusDLQ, 10, 0)
	if len(items) != 2 {
		t.Fatalf("dlq items = %d, want 2", len(items))
	}
	for _, it := range items {
		if err := st.Requeue(ctx, it.ID, "supervisor"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Requeue(ctx, items[0].ID, "supervisor"); err == nil {
		t.Fatal("requeueing an item that is not in the DLQ must fail")
	}

	_, healthy := newFake(t, fakeupstreamOK())
	p := drain(t, st, b.ID, newWorker(st, healthy, fastConfig("w-2")))
	if p.Counts[batch.StatusApplied] != 2 {
		t.Fatalf("applied after requeue = %d, want 2", p.Counts[batch.StatusApplied])
	}
	if n := count(t, st, `SELECT count(*) FROM outbox WHERE event_type = 'item.requeued' AND actor = 'supervisor'`); n != 2 {
		t.Fatalf("requeue events = %d, want 2", n)
	}
}
