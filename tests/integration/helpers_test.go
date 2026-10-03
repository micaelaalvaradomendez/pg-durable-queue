package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/fakeupstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/upstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/worker"
)

// openStore connects to TEST_DATABASE_URL and recreates the schema.
// Tests are skipped when the variable is unset so `go test ./...` works offline.
func openStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set: skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx,
		`DROP TABLE IF EXISTS outbox, item_attempt, batch_item, batch, scan_buffer CASCADE`); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return st
}

// newBatch scans n orders (refs from refFn) and confirms them as one batch.
func newBatch(t *testing.T, st *store.Store, n int, refFn func(int) string) batch.Batch {
	t.Helper()
	ctx := context.Background()
	session := fmt.Sprintf("session-%d", time.Now().UnixNano())
	for i := range n {
		if _, err := st.AddScan(ctx, session, refFn(i)); err != nil {
			t.Fatal(err)
		}
	}
	b, created, err := st.ConfirmBatch(ctx, store.ConfirmInput{
		IdempotencyKey: "key-" + session, SessionID: session,
		TargetWarehouse: "WH-NORTH", Actor: "tester",
	})
	if err != nil || !created {
		t.Fatalf("confirm: created=%v err=%v", created, err)
	}
	return b
}

func orderRef(i int) string { return fmt.Sprintf("ORD-%04d", i) }

// fastConfig keeps retries quick so tests finish in milliseconds.
func fastConfig(id string) worker.Config {
	cfg := worker.DefaultConfig(id)
	cfg.BatchSize = 20
	cfg.Lease = 5 * time.Second
	cfg.MaxAttempts = 4
	cfg.BaseBackoff, cfg.MaxBackoff = 5*time.Millisecond, 20*time.Millisecond
	cfg.PollInterval = 5 * time.Millisecond
	return cfg
}

func newFake(t *testing.T, cfg fakeupstream.Config) (*fakeupstream.Server, *upstream.Client) {
	t.Helper()
	fake := fakeupstream.New(cfg, 7)
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return fake, upstream.NewClient(srv.URL, 2*time.Second)
}

func newWorker(st *store.Store, client *upstream.Client, cfg worker.Config) *worker.Worker {
	return worker.New(cfg, st, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// drain runs the workers until the batch has no non-terminal items.
func drain(t *testing.T, st *store.Store, batchID uuid.UUID, workers ...*worker.Worker) batch.Progress {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		for _, w := range workers {
			if _, err := w.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
		}
		p, err := st.GetProgress(ctx, batchID)
		if err != nil {
			t.Fatal(err)
		}
		if p.Done {
			return p
		}
		select {
		case <-ctx.Done():
			t.Fatalf("batch not drained: %v", p.Counts)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func count(t *testing.T, st *store.Store, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := st.Pool().QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
