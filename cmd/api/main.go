// Command api serves the self-service REST API.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/api"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/orchestrator"
)

func main() {
	var (
		addr         = flag.String("addr", env("API_ADDR", ":8080"), "HTTP listen address")
		temporalAddr = flag.String("temporal", env("TEMPORAL_ADDRESS", "localhost:7233"), "Temporal frontend address")
		namespace    = flag.String("namespace", env("TEMPORAL_NAMESPACE", "default"), "Temporal namespace")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	orch, err := orchestrator.Dial(*temporalAddr, *namespace)
	if err != nil {
		log.Error("temporal connection failed", "error", err)
		os.Exit(1)
	}
	defer orch.Close()

	bus, err := events.NewFromEnv()
	if err != nil {
		log.Error("event bus init failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           (&api.Server{Orch: orch, Bus: bus, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Info("api listening", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
