package worker

import (
	"testing"
	"time"
)

func TestBackoffStaysWithinBounds(t *testing.T) {
	base, maxDelay := 2*time.Second, 2*time.Minute
	cases := []struct {
		attempt int
		ceiling time.Duration
	}{
		{0, 2 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{7, 2 * time.Minute},
		{50, 2 * time.Minute},
	}
	for _, c := range cases {
		for range 200 {
			got := Backoff(c.attempt, base, maxDelay)
			if got < c.ceiling/2 || got > c.ceiling {
				t.Fatalf("attempt %d: backoff %v outside [%v, %v]", c.attempt, got, c.ceiling/2, c.ceiling)
			}
		}
	}
}

func TestBackoffIsJittered(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[Backoff(4, time.Second, time.Minute)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected spread-out delays, got %d distinct values", len(seen))
	}
}
