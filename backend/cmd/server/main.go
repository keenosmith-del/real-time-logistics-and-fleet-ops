package main

import (
	"context"
	"fleetops/internal/platform"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var a *platform.App
	var err error
	for i := 0; i < 30; i++ {
		a, err = platform.New(ctx)
		if err == nil {
			break
		}
		slog.Warn("waiting for database", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		slog.Error("startup", "error", err)
		os.Exit(1)
	}
	defer a.DB.Close()
	defer a.Redis.Close()
	defer a.Writer.Close()
	if err = a.Migrate(ctx, platform.Env("MIGRATION_PATH", "migrations/001_init.sql")); err != nil {
		slog.Error("migration", "error", err)
		os.Exit(1)
	}
	if platform.Env("AUTO_SEED", "true") == "true" {
		if err = a.Seed(ctx); err != nil {
			slog.Error("seed", "error", err)
			os.Exit(1)
		}
	}
	if err = a.Start(ctx); err != nil {
		slog.Error("stream startup", "error", err)
		os.Exit(1)
	}
	server := &http.Server{Addr: ":8080", Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second}
	go func() {
		<-ctx.Done()
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		server.Shutdown(stop)
	}()
	slog.Info("fleet operations ready", "address", ":8080")
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http", "error", err)
		os.Exit(1)
	}
}
