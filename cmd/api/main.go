// Command api serves the scan buffer, batch confirmation and progress endpoints.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/envcfg"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/httpapi"
	"github.com/micaelaalvaradomendez/pg-durable-queue/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn, err := envcfg.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	tokens, err := envcfg.Tokens("API_TOKENS")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              envcfg.String("LISTEN_ADDR", ":8080"),
		Handler:           (&httpapi.API{Store: st, Tokens: tokens, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("api listening", "addr", srv.Addr)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
