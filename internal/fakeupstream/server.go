// Package fakeupstream simulates an unreliable warehouse-assignment API:
// slow responses, transient failures and connections dropped after the change
// was already applied.
package fakeupstream

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config controls the failure modes. Deterministic knobs (FailFirstN,
// DropAfterApplyFirst, FailAlways) are meant for tests; rates for simulations.
type Config struct {
	Latency             time.Duration
	FailureRate         float64 // 503 without applying
	DropAfterApplyRate  float64 // apply, then close the connection without answering
	FailFirstN          int     // first N requests per key fail with 503
	DropAfterApplyFirst bool    // first request per key applies and then drops
	FailAlways          bool
}

// Server is an in-memory upstream. Orders prefixed "INVALID-" are rejected with 422.
type Server struct {
	cfg Config
	mu  sync.Mutex
	// applied maps idempotency key to the order it moved.
	applied map[string]string
	// applications counts effective moves per order: >1 means a duplicate.
	applications map[string]int
	requests     map[string]int
	rng          *rand.Rand
}

// New creates a server with the given failure configuration.
func New(cfg Config, seed uint64) *Server {
	return &Server{
		cfg:          cfg,
		applied:      map[string]string{},
		applications: map[string]int{},
		requests:     map[string]int{},
		rng:          rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /assignments", s.assign)
	mux.HandleFunc("GET /assignments/{key}", s.status)
	mux.HandleFunc("GET /stats", s.stats)
	return mux
}

func (s *Server) assign(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	var req struct {
		OrderRef  string `json:"order_ref"`
		Warehouse string `json:"warehouse"`
	}
	if key == "" || json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req) != nil || req.OrderRef == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	time.Sleep(s.cfg.Latency)

	s.mu.Lock()
	s.requests[key]++
	n := s.requests[key]
	if _, done := s.applied[key]; done {
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK) // idempotent replay: same result, no new effect
		return
	}
	if strings.HasPrefix(req.OrderRef, "INVALID-") {
		s.mu.Unlock()
		http.Error(w, "unknown order", http.StatusUnprocessableEntity)
		return
	}
	if s.cfg.FailAlways || n <= s.cfg.FailFirstN || s.rng.Float64() < s.cfg.FailureRate {
		s.mu.Unlock()
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	s.applied[key] = req.OrderRef
	s.applications[req.OrderRef]++
	drop := (s.cfg.DropAfterApplyFirst && n == 1) || s.rng.Float64() < s.cfg.DropAfterApplyRate
	s.mu.Unlock()

	if drop {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	_, ok := s.applied[r.PathValue("key")]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Stats is a snapshot of what the upstream observed.
type Stats struct {
	Requests    int `json:"requests"`
	Applied     int `json:"applied"`
	Duplicates  int `json:"duplicates"`
	KeysRetried int `json:"keys_retried"`
	MaxPerKey   int `json:"max_requests_per_key"`
}

// Snapshot summarizes the traffic received so far.
func (s *Server) Snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st Stats
	for _, n := range s.requests {
		st.Requests += n
		if n > 1 {
			st.KeysRetried++
		}
		st.MaxPerKey = max(st.MaxPerKey, n)
	}
	for _, n := range s.applications {
		st.Applied++
		st.Duplicates += n - 1
	}
	return st
}

// Requests returns how many calls were received for an idempotency key.
func (s *Server) Requests(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[key]
}

// Applications returns how many times an order was effectively moved.
func (s *Server) Applications(orderRef string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applications[orderRef]
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Snapshot())
}
