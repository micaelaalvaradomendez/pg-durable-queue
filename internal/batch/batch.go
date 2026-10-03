// Package batch holds the domain model for bulk reassignment batches.
package batch

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// MaxItemsPerBatch caps how many scanned orders a single batch may contain.
const MaxItemsPerBatch = 300

// Status is the lifecycle state of a single batch item.
type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusRetry      Status = "retry"
	StatusApplied    Status = "applied"
	StatusDLQ        Status = "dlq"
)

// Terminal reports whether no worker will touch the item again without manual action.
func (s Status) Terminal() bool {
	return s == StatusApplied || s == StatusDLQ
}

// Outcome classifies one attempt against the upstream system.
type Outcome string

const (
	OutcomeApplied        Outcome = "applied"
	OutcomeAlreadyApplied Outcome = "already_applied"
	OutcomeTransient      Outcome = "transient"
	OutcomePermanent      Outcome = "permanent"
)

var (
	ErrEmptyBuffer         = errors.New("scan buffer is empty")
	ErrBatchTooLarge       = errors.New("batch exceeds the maximum number of items")
	ErrIdempotencyConflict = errors.New("idempotency key already used by another session")
	ErrNotFound            = errors.New("not found")
	ErrNotInDLQ            = errors.New("item is not in the dead letter queue")
)

// Batch is a confirmed group of orders to be reassigned to one warehouse.
type Batch struct {
	ID              uuid.UUID `json:"id"`
	IdempotencyKey  string    `json:"idempotency_key"`
	SessionID       string    `json:"session_id"`
	TargetWarehouse string    `json:"target_warehouse"`
	Note            string    `json:"note,omitempty"`
	CreatedBy       string    `json:"created_by"`
	CreatedAt       time.Time `json:"created_at"`
	ItemCount       int       `json:"item_count"`
}

// Progress summarizes how far a batch has been processed.
type Progress struct {
	Batch  Batch          `json:"batch"`
	Counts map[Status]int `json:"counts"`
	Done   bool           `json:"done"`
}

// Item is one order inside a batch.
type Item struct {
	ID        uuid.UUID `json:"id"`
	BatchID   uuid.UUID `json:"batch_id"`
	OrderRef  string    `json:"order_ref"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ClaimedItem is an item leased to a worker. Attempt doubles as a fencing token:
// a worker whose lease expired cannot overwrite the result of the next claim.
type ClaimedItem struct {
	ID              uuid.UUID
	BatchID         uuid.UUID
	OrderRef        string
	TargetWarehouse string
	Attempt         int
}
