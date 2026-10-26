package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestPostMortemAbortedTransaction25P02 reproduces the FAT-ERR-02 incident:
// In PostgreSQL, any failed statement inside a transaction block marks the entire
// transaction aborted. Subsequent commands fail with SQLSTATE 25P02 until a ROLLBACK
// is issued. The remediated pattern executes defer tx.Rollback() and isolates error
// recording in a fresh transaction.
func TestPostMortemAbortedTransaction25P02(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	b := newBatch(t, st, 1, orderRef)

	pool := st.Pool()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 1. Trigger an intentional failure (check constraint violation: invalid status)
	_, err = tx.Exec(ctx, `
		INSERT INTO batch_item (id, batch_id, order_ref, status)
		VALUES ($1, $2, $3, 'invalid_status')`,
		uuid.Must(uuid.NewV7()), b.ID, "ORD-ERR-TEST")
	if err == nil {
		t.Fatal("expected constraint error, got nil")
	}

	// 2. ANTIPATTERN: Attempt to run another query on the aborted transaction without rollback.
	// This simulates a worker trying to record the failure using the corrupted connection/tx.
	_, secondErr := tx.Exec(ctx, "SELECT 1")
	if secondErr == nil {
		t.Fatal("expected SQLSTATE 25P02, got nil")
	}

	var pgErr *pgconn.PgError
	if !errors.As(secondErr, &pgErr) {
		t.Fatalf("expected pgconn.PgError, got: %T (%v)", secondErr, secondErr)
	}

	const sqlstateAbortedTransaction = "25P02"
	if pgErr.Code != sqlstateAbortedTransaction {
		t.Fatalf("expected SQLSTATE %s (in_failed_sql_transaction), got: %s (%v)",
			sqlstateAbortedTransaction, pgErr.Code, pgErr.Message)
	}

	// 3. REMEDIATION: Roll back the failed transaction and record failure in a clean transaction.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	cleanTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanTx.Rollback(ctx) //nolint:errcheck

	var ping int
	if err := cleanTx.QueryRow(ctx, "SELECT 1").Scan(&ping); err != nil {
		t.Fatalf("clean transaction failed: %v", err)
	}
	if ping != 1 {
		t.Fatalf("expected ping=1, got %d", ping)
	}
	if err := cleanTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestPostMortemContextWithoutCancelForBackgroundWork reproduces the FAT-ERR-03/FAT-ERR-04 incident:
// When background tasks (outbox writes, logging, auditing) are dispatched from an HTTP handler,
// using the client's request context will abort the background task with context.Canceled if the client
// disconnects or the request completes. context.WithoutCancel preserves context values (e.g. actor, trace IDs)
// while detaching the cancellation deadline, ensuring background work commits cleanly.
func TestPostMortemContextWithoutCancelForBackgroundWork(t *testing.T) {
	st := openStore(t)

	// Simulate an incoming HTTP request context that gets cancelled (e.g. client disconnect / handler exit)
	reqCtx, cancelReq := context.WithCancel(context.Background())

	// 1. ANTIPATTERN: Passing the request context directly to background work.
	// When the request terminates, any in-flight or subsequent query on that context fails.
	cancelReq() // Client disconnects / request finished

	sessionAntipattern := "session-cancelled"
	_, err := st.AddScan(reqCtx, sessionAntipattern, "ORD-ANTIPATTERN-001")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on cancelled request context, got: %v", err)
	}

	// 2. REMEDIATION: context.WithoutCancel allows background tasks to outlive the client request
	// while detaching the parent's cancellation signal.
	parentCtx, cancelParent := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelParent()
	bgCtx := context.WithoutCancel(parentCtx)

	// Wait for the parent context to expire
	time.Sleep(20 * time.Millisecond)
	if parentCtx.Err() == nil {
		t.Fatal("expected parent context to be expired")
	}

	// bgCtx is NOT cancelled and can safely persist background events
	sessionClean := "session-bg-clean"
	added, err := st.AddScan(bgCtx, sessionClean, "ORD-CLEAN-001")
	if err != nil {
		t.Fatalf("expected AddScan to succeed with context.WithoutCancel, got: %v", err)
	}
	if !added {
		t.Fatal("expected scan to be added")
	}

	// Verify the scan was persisted in PostgreSQL despite the parent context being dead
	scans := count(t, st, "SELECT count(*) FROM scan_buffer WHERE session_id = $1", sessionClean)
	if scans != 1 {
		t.Fatalf("expected 1 persisted scan, got %d", scans)
	}
}
