// Command fake-upstream runs the unreliable warehouse API used in demos.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/envcfg"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/fakeupstream"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := fakeupstream.Config{
		Latency:            envcfg.Duration("FAKE_LATENCY", 50*time.Millisecond),
		FailureRate:        envcfg.Float("FAKE_FAILURE_RATE", 0.3),
		DropAfterApplyRate: envcfg.Float("FAKE_DROP_AFTER_APPLY_RATE", 0.1),
	}
	srv := &http.Server{
		Addr:              envcfg.String("LISTEN_ADDR", ":9090"),
		Handler:           fakeupstream.New(cfg, uint64(time.Now().UnixNano())).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("fake upstream listening", "addr", srv.Addr, "failure_rate", cfg.FailureRate, "drop_after_apply_rate", cfg.DropAfterApplyRate)
	if err := srv.ListenAndServe(); err != nil {
		log.Error("fake upstream stopped", "err", err)
		os.Exit(1)
	}
}
