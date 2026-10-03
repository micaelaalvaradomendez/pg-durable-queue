package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAssignClassifiesResponses(t *testing.T) {
	cases := []struct {
		status    int
		wantErr   bool
		permanent bool
	}{
		{http.StatusCreated, false, false},
		{http.StatusOK, false, false},
		{http.StatusServiceUnavailable, true, false},
		{http.StatusTooManyRequests, true, false},
		{http.StatusUnprocessableEntity, true, true},
		{http.StatusBadRequest, true, true},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Idempotency-Key") != "k-1" {
				t.Errorf("missing idempotency key")
			}
			w.WriteHeader(c.status)
		}))
		err := NewClient(srv.URL, time.Second).Assign(context.Background(), "k-1", "ORD-1", "WH-1")
		srv.Close()
		if (err != nil) != c.wantErr {
			t.Fatalf("status %d: err=%v, wantErr=%v", c.status, err, c.wantErr)
		}
		if IsPermanent(err) != c.permanent {
			t.Fatalf("status %d: permanent=%v, want %v", c.status, IsPermanent(err), c.permanent)
		}
	}
}

func TestAssignTimeoutIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	err := NewClient(srv.URL, 20*time.Millisecond).Assign(context.Background(), "k", "ORD-1", "WH-1")
	if err == nil || IsPermanent(err) {
		t.Fatalf("timeout should be a transient error, got %v", err)
	}
}
