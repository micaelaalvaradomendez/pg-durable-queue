package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
)

// ClaimItems leases up to limit due items to workerID. Items whose lease expired
// (a worker crashed mid-flight) become claimable again. SKIP LOCKED lets several
// workers claim concurrently without ever receiving the same row.
func (s *Store) ClaimItems(ctx context.Context, limit int, lease time.Duration) ([]batch.ClaimedItem, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM batch_item
			WHERE (status IN ('pending', 'retry') AND next_attempt_at <= now())
			   OR (status = 'in_progress' AND lease_until < now())
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE batch_item i
		SET status = 'in_progress',
		    lease_until = now() + make_interval(secs => $2),
		    attempts = i.attempts + 1,
		    updated_at = now()
		FROM due, batch b
		WHERE i.id = due.id AND b.id = i.batch_id
		RETURNING i.id, i.batch_id, i.order_ref, b.target_warehouse, i.attempts`,
		limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var claimed []batch.ClaimedItem
	for rows.Next() {
		var c batch.ClaimedItem
		if err := rows.Scan(&c.ID, &c.BatchID, &c.OrderRef, &c.TargetWarehouse, &c.Attempt); err != nil {
			return nil, err
		}
		claimed = append(claimed, c)
	}
	return claimed, rows.Err()
}

// Result is what a worker reports back for a claimed item.
type Result struct {
	Item     batch.ClaimedItem
	WorkerID string
	Outcome  batch.Outcome
	Detail   string
	// Next is the new status: applied, retry or dlq.
	Next      batch.Status
	NextRetry time.Time
}

// Complete records the attempt and moves the item to its next status.
// It returns false when the lease was lost (another worker reclaimed the item):
// the stale result is discarded and nothing is written.
func (s *Store) Complete(ctx context.Context, r Result) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var nextAttempt any
	if r.Next == batch.StatusRetry {
		nextAttempt = r.NextRetry
	}
	tag, err := tx.Exec(ctx, `
		UPDATE batch_item
		SET status = $3,
		    lease_until = NULL,
		    next_attempt_at = COALESCE($4, next_attempt_at),
		    last_error = CASE WHEN $3 = 'applied' THEN NULL ELSE NULLIF($5, '') END,
		    updated_at = now()
		WHERE id = $1 AND attempts = $2 AND status = 'in_progress'`,
		r.Item.ID, r.Item.Attempt, string(r.Next), nextAttempt, r.Detail)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO item_attempt (id, item_id, attempt_no, worker_id, outcome, detail)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))`,
		newID(), r.Item.ID, r.Item.Attempt, r.WorkerID, string(r.Outcome), r.Detail); err != nil {
		return false, err
	}

	if r.Next == batch.StatusDLQ {
		if err := insertEvent(ctx, tx, r.Item.ID, "item.dead_lettered", r.WorkerID, map[string]any{
			"batch_id": r.Item.BatchID,
			"order":    r.Item.OrderRef,
			"attempts": r.Item.Attempt,
			"reason":   r.Detail,
		}); err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

// Requeue moves a dead-lettered item back to the queue with a fresh retry budget.
func (s *Store) Requeue(ctx context.Context, itemID uuid.UUID, actor string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var batchID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE batch_item
		SET status = 'retry', attempts = 0, next_attempt_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'dlq'
		RETURNING batch_id`, itemID).Scan(&batchID)
	if err == pgx.ErrNoRows {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM batch_item WHERE id = $1)`, itemID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return batch.ErrNotFound
		}
		return batch.ErrNotInDLQ
	}
	if err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, itemID, "item.requeued", actor, map[string]any{"batch_id": batchID}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
