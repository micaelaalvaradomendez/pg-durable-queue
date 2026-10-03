// Package httpapi exposes the scan buffer, batch confirmation and progress endpoints.
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/batch"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
)

const maxBodyBytes = 16 << 10

var (
	orderRefPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9-]{2,39}$`)
	tokenPattern    = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)
)

// Store is the persistence the API depends on.
type Store interface {
	AddScan(ctx context.Context, sessionID, orderRef string) (bool, error)
	ConfirmBatch(ctx context.Context, in store.ConfirmInput) (batch.Batch, bool, error)
	GetProgress(ctx context.Context, batchID uuid.UUID) (batch.Progress, error)
	ListItems(ctx context.Context, batchID uuid.UUID, status batch.Status, limit, offset int) ([]batch.Item, error)
	Requeue(ctx context.Context, itemID uuid.UUID, actor string) error
}

// API wires handlers to the store. Tokens maps bearer tokens to actor names:
// the actor always comes from the authenticated token, never from the request body.
type API struct {
	Store  Store
	Tokens map[string]string
	Log    *slog.Logger
}

type ctxKey struct{}

// Handler returns the routed, authenticated handler.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sessions/{session}/scans", a.addScan)
	mux.HandleFunc("POST /batches", a.confirm)
	mux.HandleFunc("GET /batches/{id}", a.progress)
	mux.HandleFunc("GET /batches/{id}/items", a.items)
	mux.HandleFunc("POST /items/{id}/requeue", a.requeue)
	return a.authenticate(mux)
}

func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		actor := ""
		if ok {
			for t, name := range a.Tokens {
				if subtle.ConstantTimeCompare([]byte(t), []byte(token)) == 1 {
					actor = name
				}
			}
		}
		if actor == "" {
			writeError(w, http.StatusUnauthorized, "missing or invalid token")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, actor)))
	})
}

func actorFrom(r *http.Request) string {
	actor, _ := r.Context().Value(ctxKey{}).(string)
	return actor
}

func (a *API) addScan(w http.ResponseWriter, r *http.Request) {
	session := r.PathValue("session")
	var body struct {
		OrderRef string `json:"order_ref"`
	}
	if !tokenPattern.MatchString(session) || !decode(w, r, &body) {
		return
	}
	if !orderRefPattern.MatchString(body.OrderRef) {
		writeError(w, http.StatusUnprocessableEntity, "order_ref must match "+orderRefPattern.String())
		return
	}
	added, err := a.Store.AddScan(r.Context(), session, body.OrderRef)
	if err != nil {
		a.internal(w, err)
		return
	}
	status := http.StatusOK
	if added {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"order_ref": body.OrderRef, "added": added})
}

func (a *API) confirm(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if !tokenPattern.MatchString(key) {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var body struct {
		SessionID       string `json:"session_id"`
		TargetWarehouse string `json:"target_warehouse"`
		Note            string `json:"note"`
	}
	if !decode(w, r, &body) {
		return
	}
	if !tokenPattern.MatchString(body.SessionID) || !orderRefPattern.MatchString(body.TargetWarehouse) || len(body.Note) > 500 {
		writeError(w, http.StatusUnprocessableEntity, "invalid session_id, target_warehouse or note")
		return
	}
	b, created, err := a.Store.ConfirmBatch(r.Context(), store.ConfirmInput{
		IdempotencyKey:  key,
		SessionID:       body.SessionID,
		TargetWarehouse: body.TargetWarehouse,
		Note:            body.Note,
		Actor:           actorFrom(r),
	})
	switch {
	case errors.Is(err, batch.ErrEmptyBuffer), errors.Is(err, batch.ErrBatchTooLarge):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, batch.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		a.internal(w, err)
	case created:
		writeJSON(w, http.StatusCreated, b)
	default:
		writeJSON(w, http.StatusOK, b)
	}
}

func (a *API) progress(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	p, err := a.Store.GetProgress(r.Context(), id)
	if errors.Is(err, batch.ErrNotFound) {
		writeError(w, http.StatusNotFound, "batch not found")
		return
	}
	if err != nil {
		a.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (a *API) items(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	status := batch.Status(q.Get("status"))
	switch status {
	case "", batch.StatusPending, batch.StatusInProgress, batch.StatusRetry, batch.StatusApplied, batch.StatusDLQ:
	default:
		writeError(w, http.StatusBadRequest, "unknown status")
		return
	}
	limit := clampInt(q.Get("limit"), 50, 1, 200)
	offset := clampInt(q.Get("offset"), 0, 0, batch.MaxItemsPerBatch)
	items, err := a.Store.ListItems(r.Context(), id, status, limit, offset)
	if err != nil {
		a.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func (a *API) requeue(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	err := a.Store.Requeue(r.Context(), id, actorFrom(r))
	switch {
	case errors.Is(err, batch.ErrNotFound):
		writeError(w, http.StatusNotFound, "item not found")
	case errors.Is(err, batch.ErrNotInDLQ):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		a.internal(w, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *API) internal(w http.ResponseWriter, err error) {
	a.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return uuid.Nil, false
	}
	return id, true
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func clampInt(raw string, def, lo, hi int) int {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return max(lo, min(n, hi))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
