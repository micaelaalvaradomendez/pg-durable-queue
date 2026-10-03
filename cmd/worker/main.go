// Command worker drains the queue against the upstream API.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/envcfg"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/upstream"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn, err := envcfg.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	upstreamURL, err := envcfg.Required("UPSTREAM_URL")
	if err != nil {
		return err
	}
	hostname, _ := os.Hostname()

	cfg := worker.DefaultConfig(envcfg.String("WORKER_ID", hostname))
	cfg.BatchSize = envcfg.Int("WORKER_BATCH_SIZE", cfg.BatchSize)
	cfg.MaxAttempts = envcfg.Int("WORKER_MAX_ATTEMPTS", cfg.MaxAttempts)
	cfg.Lease = envcfg.Duration("WORKER_LEASE", cfg.Lease)
	cfg.BaseBackoff = envcfg.Duration("WORKER_BASE_BACKOFF", cfg.BaseBackoff)
	cfg.MaxBackoff = envcfg.Duration("WORKER_MAX_BACKOFF", cfg.MaxBackoff)
	cfg.PollInterval = envcfg.Duration("WORKER_POLL_INTERVAL", cfg.PollInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	client := upstream.NewClient(upstreamURL, envcfg.Duration("UPSTREAM_TIMEOUT", 5*time.Second))
	log.Info("worker started", "worker", cfg.ID, "batch_size", cfg.BatchSize, "max_attempts", cfg.MaxAttempts)
	return worker.New(cfg, st, client, log).Run(ctx)
}
