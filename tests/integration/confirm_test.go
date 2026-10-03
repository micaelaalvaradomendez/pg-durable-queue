package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
)

func TestConfirmBatchIsAtomicAndIdempotent(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	for i := range 5 {
		if _, err := st.AddScan(ctx, "s-1", orderRef(i)); err != nil {
			t.Fatal(err)
		}
	}
	if added, _ := st.AddScan(ctx, "s-1", orderRef(0)); added {
		t.Fatal("rescanning the same order must be a no-op")
	}

	in := store.ConfirmInput{IdempotencyKey: "k-1", SessionID: "s-1", TargetWarehouse: "WH-1", Actor: "ana"}
	first, created, err := st.ConfirmBatch(ctx, in)
	if err != nil || !created {
		t.Fatalf("first confirm: created=%v err=%v", created, err)
	}
	replay, created, err := st.ConfirmBatch(ctx, in)
	if err != nil || created || replay.ID != first.ID {
		t.Fatalf("replay must return the same batch: created=%v id=%v err=%v", created, replay.ID, err)
	}

	if n := count(t, st, `SELECT count(*) FROM batch_item WHERE batch_id = $1`, first.ID); n != 5 {
		t.Fatalf("items = %d, want 5", n)
	}
	if n := count(t, st, `SELECT count(*) FROM scan_buffer WHERE session_id = 's-1' AND deleted_at IS NULL`); n != 0 {
		t.Fatalf("buffer still has %d active rows", n)
	}
	if n := count(t, st, `SELECT count(*) FROM outbox WHERE event_type = 'batch.confirmed'`); n != 1 {
		t.Fatalf("confirmed events = %d, want 1", n)
	}

	in.SessionID = "s-other"
	if _, _, err := st.ConfirmBatch(ctx, in); !errors.Is(err, batch.ErrIdempotencyConflict) {
		t.Fatalf("reusing a key from another session: err=%v", err)
	}
}

func TestConcurrentConfirmsWithSameKeyCreateOneBatch(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	for i := range 20 {
		if _, err := st.AddScan(ctx, "s-race", orderRef(i)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, err := st.ConfirmBatch(ctx, store.ConfirmInput{
				IdempotencyKey: "k-race", SessionID: "s-race", TargetWarehouse: "WH-1", Actor: "ana"})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- b.ID.String()
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("expected one batch id, got %d", len(seen))
	}
	if n := count(t, st, `SELECT count(*) FROM batch_item`); n != 20 {
		t.Fatalf("items = %d, want 20", n)
	}
}

func TestConfirmBatchRejectsOversizedAndEmptyBuffers(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	_, _, err := st.ConfirmBatch(ctx, store.ConfirmInput{IdempotencyKey: "k-empty", SessionID: "s-empty", TargetWarehouse: "WH-1", Actor: "ana"})
	if !errors.Is(err, batch.ErrEmptyBuffer) {
		t.Fatalf("empty buffer: err=%v", err)
	}

	for i := range batch.MaxItemsPerBatch + 1 {
		if _, err := st.AddScan(ctx, "s-big", fmt.Sprintf("ORD-%05d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = st.ConfirmBatch(ctx, store.ConfirmInput{IdempotencyKey: "k-big", SessionID: "s-big", TargetWarehouse: "WH-1", Actor: "ana"})
	if !errors.Is(err, batch.ErrBatchTooLarge) {
		t.Fatalf("oversized batch: err=%v", err)
	}
	if n := count(t, st, `SELECT count(*) FROM batch`); n != 0 {
		t.Fatalf("a rejected confirmation must not leave a batch behind, found %d", n)
	}
	if n := count(t, st, `SELECT count(*) FROM scan_buffer WHERE deleted_at IS NULL`); n != batch.MaxItemsPerBatch+1 {
		t.Fatalf("buffer must stay intact, active rows = %d", n)
	}
}

func TestAttemptLogIsAppendOnly(t *testing.T) {
	st := openStore(t)
	b := newBatch(t, st, 1, orderRef)
	_, client := newFake(t, fakeupstreamOK())
	drain(t, st, b.ID, newWorker(st, client, fastConfig("w-1")))

	_, err := st.Pool().Exec(context.Background(), `UPDATE item_attempt SET outcome = 'permanent'`)
	if err == nil {
		t.Fatal("updating the attempt log must fail")
	}
	_, err = st.Pool().Exec(context.Background(), `DELETE FROM item_attempt`)
	if err == nil {
		t.Fatal("deleting from the attempt log must fail")
	}
}
