// Package worker drains the queue: it claims due items, calls upstream and
// records the outcome.
package worker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/upstream"
)

// Queue is the persistence the worker needs.
type Queue interface {
	ClaimItems(ctx context.Context, limit int, lease time.Duration) ([]batch.ClaimedItem, error)
	Complete(ctx context.Context, r store.Result) (bool, error)
}

// Upstream is the external API the worker talks to.
type Upstream interface {
	Assign(ctx context.Context, idempotencyKey, orderRef, warehouse string) error
	Applied(ctx context.Context, idempotencyKey string) (bool, error)
}

// Config tunes throughput and retry policy.
type Config struct {
	ID           string
	BatchSize    int
	Lease        time.Duration
	MaxAttempts  int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	PollInterval time.Duration
}

// DefaultConfig is a sensible starting point: 5 attempts spread over a few minutes.
func DefaultConfig(id string) Config {
	return Config{
		ID:           id,
		BatchSize:    50,
		Lease:        60 * time.Second,
		MaxAttempts:  5,
		BaseBackoff:  2 * time.Second,
		MaxBackoff:   2 * time.Minute,
		PollInterval: time.Second,
	}
}

// Worker processes items one claim at a time.
type Worker struct {
	cfg Config
	q   Queue
	up  Upstream
	log *slog.Logger
	now func() time.Time
}

// New builds a worker.
func New(cfg Config, q Queue, up Upstream, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, q: q, up: up, log: log, now: time.Now}
}

// Run polls until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		n, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("worker cycle failed", "worker", w.cfg.ID, "err", err)
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// RunOnce claims one batch of due items and processes them. It returns how many
// items were claimed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	items, err := w.q.ClaimItems(ctx, w.cfg.BatchSize, w.cfg.Lease)
	if err != nil {
		return 0, err
	}
	for _, it := range items {
		if ctx.Err() != nil {
			// Unprocessed items stay leased and are reclaimed after the lease expires.
			return len(items), ctx.Err()
		}
		res := w.process(ctx, it)
		ok, err := w.q.Complete(ctx, res)
		if err != nil {
			return len(items), err
		}
		if !ok {
			w.log.Warn("lease lost, result discarded", "worker", w.cfg.ID, "item", it.ID, "attempt", it.Attempt)
		}
	}
	return len(items), nil
}

func (w *Worker) process(ctx context.Context, it batch.ClaimedItem) store.Result {
	key := it.ID.String()
	res := store.Result{Item: it, WorkerID: w.cfg.ID}

	err := w.up.Assign(ctx, key, it.OrderRef, it.TargetWarehouse)
	switch {
	case err == nil:
		res.Outcome, res.Next = batch.OutcomeApplied, batch.StatusApplied
		return res
	case upstream.IsPermanent(err):
		res.Outcome, res.Next, res.Detail = batch.OutcomePermanent, batch.StatusDLQ, err.Error()
		return res
	}

	// Ambiguous failure: the change may have been applied before the response was lost.
	if applied, lookupErr := w.up.Applied(ctx, key); lookupErr == nil && applied {
		res.Outcome, res.Next = batch.OutcomeAlreadyApplied, batch.StatusApplied
		res.Detail = "confirmed by status lookup after: " + err.Error()
		return res
	}

	res.Outcome, res.Detail = batch.OutcomeTransient, err.Error()
	if it.Attempt >= w.cfg.MaxAttempts {
		res.Next = batch.StatusDLQ
		res.Detail = "retries exhausted: " + err.Error()
		return res
	}
	res.Next = batch.StatusRetry
	res.NextRetry = w.now().Add(Backoff(it.Attempt, w.cfg.BaseBackoff, w.cfg.MaxBackoff))
	return res
}

// Backoff returns an exponential delay with equal jitter: a random value in
// [d/2, d] where d = min(maxDelay, base * 2^(attempt-1)). Jitter keeps retries
// of a whole failed batch from hitting upstream at the same instant.
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
