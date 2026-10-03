// Package store persists batches, items and their attempt log in PostgreSQL.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store wraps a connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool exposes the underlying pool (used by tests and tooling).
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Migrate applies the embedded migrations in lexical order. They are idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := migrationFS.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func newID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// AddScan stores a scanned order in the session buffer. Rescanning is a no-op.
func (s *Store) AddScan(ctx context.Context, sessionID, orderRef string) (added bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO scan_buffer (id, session_id, order_ref)
		VALUES ($1, $2, $3)
		ON CONFLICT (session_id, order_ref) WHERE deleted_at IS NULL DO NOTHING`,
		newID(), sessionID, orderRef)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ConfirmInput is what an operator submits to turn a buffer into a batch.
type ConfirmInput struct {
	IdempotencyKey  string
	SessionID       string
	TargetWarehouse string
	Note            string
	Actor           string
}

// ConfirmBatch atomically creates the batch and its items, retires the buffer and
// records the domain event. Replaying the same idempotency key returns the original batch.
func (s *Store) ConfirmBatch(ctx context.Context, in ConfirmInput) (b batch.Batch, created bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return b, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if existing, err := batchByKey(ctx, tx, in.IdempotencyKey); err == nil {
		if existing.SessionID != in.SessionID {
			return b, false, batch.ErrIdempotencyConflict
		}
		return existing, false, nil
	} else if !errors.Is(err, batch.ErrNotFound) {
		return b, false, err
	}

	rows, err := tx.Query(ctx, `
		SELECT id, order_ref FROM scan_buffer
		WHERE session_id = $1 AND deleted_at IS NULL
		ORDER BY created_at
		FOR UPDATE`, in.SessionID)
	if err != nil {
		return b, false, err
	}
	var bufferIDs []uuid.UUID
	var orderRefs []string
	for rows.Next() {
		var id uuid.UUID
		var ref string
		if err := rows.Scan(&id, &ref); err != nil {
			rows.Close()
			return b, false, err
		}
		bufferIDs = append(bufferIDs, id)
		orderRefs = append(orderRefs, ref)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, false, err
	}
	switch {
	case len(orderRefs) == 0:
		// A concurrent confirmation with the same key may have consumed the buffer
		// while we waited on the row locks: under READ COMMITTED a new statement
		// now sees its committed batch.
		if existing, err := batchByKey(ctx, tx, in.IdempotencyKey); err == nil && existing.SessionID == in.SessionID {
			return existing, false, nil
		}
		return b, false, batch.ErrEmptyBuffer
	case len(orderRefs) > batch.MaxItemsPerBatch:
		return b, false, batch.ErrBatchTooLarge
	}

	b = batch.Batch{
		ID:              newID(),
		IdempotencyKey:  in.IdempotencyKey,
		SessionID:       in.SessionID,
		TargetWarehouse: in.TargetWarehouse,
		Note:            in.Note,
		CreatedBy:       in.Actor,
		ItemCount:       len(orderRefs),
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO batch (id, idempotency_key, session_id, target_warehouse, note, created_by)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6)
		RETURNING created_at`,
		b.ID, b.IdempotencyKey, b.SessionID, b.TargetWarehouse, b.Note, b.CreatedBy,
	).Scan(&b.CreatedAt)
	if isUniqueViolation(err) {
		// A concurrent request with the same key won the race: return its batch.
		tx.Rollback(ctx) //nolint:errcheck
		existing, err := batchByKey(ctx, s.pool, in.IdempotencyKey)
		return existing, false, err
	}
	if err != nil {
		return b, false, err
	}

	itemRows := make([][]any, len(orderRefs))
	for i, ref := range orderRefs {
		itemRows[i] = []any{newID(), b.ID, ref}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"batch_item"},
		[]string{"id", "batch_id", "order_ref"}, pgx.CopyFromRows(itemRows)); err != nil {
		return b, false, err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE scan_buffer SET deleted_at = now() WHERE id = ANY($1)`, bufferIDs); err != nil {
		return b, false, err
	}

	if err := insertEvent(ctx, tx, b.ID, "batch.confirmed", in.Actor, map[string]any{
		"item_count":       b.ItemCount,
		"target_warehouse": b.TargetWarehouse,
	}); err != nil {
		return b, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return b, false, err
	}
	return b, true, nil
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const batchColumns = `b.id, b.idempotency_key, b.session_id, b.target_warehouse,
	COALESCE(b.note, ''), b.created_by, b.created_at,
	(SELECT count(*) FROM batch_item i WHERE i.batch_id = b.id)`

func scanBatch(row pgx.Row) (batch.Batch, error) {
	var b batch.Batch
	err := row.Scan(&b.ID, &b.IdempotencyKey, &b.SessionID, &b.TargetWarehouse,
		&b.Note, &b.CreatedBy, &b.CreatedAt, &b.ItemCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, batch.ErrNotFound
	}
	return b, err
}

func batchByKey(ctx context.Context, q querier, key string) (batch.Batch, error) {
	return scanBatch(q.QueryRow(ctx,
		`SELECT `+batchColumns+` FROM batch b WHERE b.idempotency_key = $1`, key))
}

// GetProgress returns the batch and its item counts per status.
func (s *Store) GetProgress(ctx context.Context, batchID uuid.UUID) (batch.Progress, error) {
	b, err := scanBatch(s.pool.QueryRow(ctx,
		`SELECT `+batchColumns+` FROM batch b WHERE b.id = $1`, batchID))
	if err != nil {
		return batch.Progress{}, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM batch_item WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return batch.Progress{}, err
	}
	defer rows.Close()
	p := batch.Progress{Batch: b, Counts: map[batch.Status]int{}}
	terminal := 0
	for rows.Next() {
		var st batch.Status
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return p, err
		}
		p.Counts[st] = n
		if st.Terminal() {
			terminal += n
		}
	}
	p.Done = terminal == b.ItemCount
	return p, rows.Err()
}

// ListItems pages through a batch's items, optionally filtered by status.
func (s *Store) ListItems(ctx context.Context, batchID uuid.UUID, status batch.Status, limit, offset int) ([]batch.Item, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, batch_id, order_ref, status, attempts, COALESCE(last_error, ''), updated_at
		FROM batch_item
		WHERE batch_id = $1 AND ($2 = '' OR status = $2)
		ORDER BY order_ref
		LIMIT $3 OFFSET $4`, batchID, string(status), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []batch.Item{}
	for rows.Next() {
		var it batch.Item
		if err := rows.Scan(&it.ID, &it.BatchID, &it.OrderRef, &it.Status,
			&it.Attempts, &it.LastError, &it.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func insertEvent(ctx context.Context, e execer, aggregateID uuid.UUID, eventType, actor string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = e.Exec(ctx, `
		INSERT INTO outbox (id, aggregate_id, event_type, actor, payload)
		VALUES ($1, $2, $3, $4, $5)`, newID(), aggregateID, eventType, actor, body)
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
