// Command simulate runs an end-to-end scenario in one process: it fills a scan
// buffer, confirms a batch and drains it with several workers against a degraded
// upstream, then prints the measured results.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/envcfg"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/fakeupstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/upstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/worker"
)

func main() {
	items := flag.Int("items", batch.MaxItemsPerBatch, "orders in the batch")
	invalid := flag.Int("invalid", 3, "orders upstream will reject permanently")
	workers := flag.Int("workers", 4, "concurrent workers")
	failure := flag.Float64("failure", 0.3, "upstream transient failure rate")
	drop := flag.Float64("drop", 0.1, "rate of connections dropped after applying")
	latency := flag.Duration("latency", 20*time.Millisecond, "upstream latency")
	flag.Parse()

	if err := run(*items, *invalid, *workers, *failure, *drop, *latency); err != nil {
		fmt.Fprintln(os.Stderr, "simulate:", err)
		os.Exit(1)
	}
}

func run(items, invalid, workers int, failure, drop float64, latency time.Duration) error {
	dsn, err := envcfg.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	fake := fakeupstream.New(fakeupstream.Config{
		Latency: latency, FailureRate: failure, DropAfterApplyRate: drop,
	}, 42)
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()

	session := "sim-" + uuid.NewString()[:8]
	for i := range items {
		ref := fmt.Sprintf("ORD-%s-%04d", session[4:], i)
		if i < invalid {
			ref = "INVALID-" + ref
		}
		if _, err := st.AddScan(ctx, session, ref); err != nil {
			return err
		}
	}

	start := time.Now()
	b, _, err := st.ConfirmBatch(ctx, store.ConfirmInput{
		IdempotencyKey: "sim-" + uuid.NewString(), SessionID: session,
		TargetWarehouse: "WH-SOUTH", Actor: "simulator",
	})
	if err != nil {
		return err
	}
	confirmLatency := time.Since(start)

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := upstream.NewClient(srv.URL, 2*time.Second)
	workCtx, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range workers {
		cfg := worker.DefaultConfig(fmt.Sprintf("sim-worker-%d", i))
		cfg.BatchSize, cfg.Lease = 25, 10*time.Second
		cfg.BaseBackoff, cfg.MaxBackoff, cfg.PollInterval = 50*time.Millisecond, time.Second, 20*time.Millisecond
		w := worker.New(cfg, st, client, quiet)
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(workCtx) }()
	}

	drainStart := time.Now()
	var p batch.Progress
	for {
		if p, err = st.GetProgress(ctx, b.ID); err != nil {
			stopWorkers()
			return err
		}
		if p.Done {
			break
		}
		select {
		case <-ctx.Done():
			stopWorkers()
			return fmt.Errorf("batch did not finish: %v", p.Counts)
		case <-time.After(50 * time.Millisecond):
		}
	}
	drain := time.Since(drainStart)
	stopWorkers()
	wg.Wait()

	var attempts, alreadyApplied int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE a.outcome = 'already_applied')
		FROM item_attempt a JOIN batch_item i ON i.id = a.item_id
		WHERE i.batch_id = $1`, b.ID).Scan(&attempts, &alreadyApplied); err != nil {
		return err
	}
	up := fake.Snapshot()

	fmt.Printf("Scenario: %d orders (%d invalid), %d workers, upstream failure=%.0f%% drop-after-apply=%.0f%% latency=%v\n",
		items, invalid, workers, failure*100, drop*100, latency)
	fmt.Printf("  Batch confirmation (1 transaction): %v\n", confirmLatency.Round(time.Millisecond))
	fmt.Printf("  Drain time: %v (%.1f orders/s)\n", drain.Round(time.Millisecond), float64(items)/drain.Seconds())
	fmt.Printf("  Applied: %d   Dead-lettered: %d\n", p.Counts[batch.StatusApplied], p.Counts[batch.StatusDLQ])
	fmt.Printf("  Attempts recorded: %d (%.2f per order)   Recovered via status lookup: %d\n",
		attempts, float64(attempts)/float64(items), alreadyApplied)
	fmt.Printf("  Upstream requests: %d   Keys retried: %d   Duplicate applications: %d\n",
		up.Requests, up.KeysRetried, up.Duplicates)
	return nil
}
