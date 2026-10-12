// Command worker runs the Temporal worker that executes the provisioning
// workflows and activities. With -remediator it also consumes health events
// and signals the owning host workflows (event-driven self-healing). With
// -sim-admin it exposes fault-injection endpoints for the simulator.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/orchestrator"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/remediation"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/workflows"
)

func main() {
	var (
		temporalAddr = flag.String("temporal", env("TEMPORAL_ADDRESS", "localhost:7233"), "Temporal frontend address")
		namespace    = flag.String("namespace", env("TEMPORAL_NAMESPACE", "default"), "Temporal namespace")
		withRemed    = flag.Bool("remediator", false, "also run the health-event remediator")
		simAdmin     = flag.String("sim-admin", "", "listen address for simulator fault injection, e.g. :9090")
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

	// The simulator stands in for BMC/PXE/node agents. Swap in a real
	// hardware.Provider here for production.
	sim := hardware.NewSimulator()
	acts := &activities.Activities{Provider: sim, Bus: bus}

	w := worker.New(orch.C, workflows.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.HostLifecycleWorkflow)
	w.RegisterWorkflow(workflows.ClusterWorkflow)
	w.RegisterActivity(acts)

	if *withRemed {
		go func() {
			log.Info("remediator started")
			if err := bus.Subscribe(ctx, remediation.Handler(orch, log)); err != nil {
				log.Error("remediator stopped", "error", err)
			}
		}()
	}
	if *simAdmin != "" {
		go serveSimAdmin(ctx, *simAdmin, sim, bus, log)
	}

	log.Info("worker started", "taskQueue", workflows.TaskQueue, "temporal", *temporalAddr)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

// serveSimAdmin lets demos break the simulated fleet:
//
//	POST /faults/degrade?host=ID   soft fault (repairable): publishes a health event
//	POST /faults/break?host=ID     permanent hardware fault: repairs fail -> RMA
func serveSimAdmin(ctx context.Context, addr string, sim *hardware.Simulator, bus events.Bus, log *slog.Logger) {
	mux := http.NewServeMux()
	fault := func(permanent bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			host := r.URL.Query().Get("host")
			var err error
			if permanent {
				err = sim.SetBroken(host)
			} else {
				err = sim.SetHealthy(host, false)
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			ev := events.Event{
				ID: "sim:" + host, Type: events.TypeHealthDegraded, HostID: host, Reason: "simulated GPU fault",
			}
			if err := bus.Publish(r.Context(), ev); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		}
	}
	mux.HandleFunc("POST /faults/degrade", fault(false))
	mux.HandleFunc("POST /faults/break", fault(true))

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	log.Info("simulator admin listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("sim admin stopped", "error", err)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
