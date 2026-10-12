// Package remediation turns health events into workflow signals. It is the
// event-driven bridge between monitoring and the self-healing workflows:
// nothing polls for sick nodes; a degraded-host event wakes the owning host
// workflow, which drains, repairs and re-validates it.
package remediation

import (
	"context"
	"log/slog"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
)

// Signaler delivers a "degraded" signal to a host's workflow.
type Signaler interface {
	SignalDegraded(ctx context.Context, hostID, reason string) error
}

// Handler returns an events.Handler that reacts to host.health.degraded and
// ignores every other event type.
func Handler(s Signaler, log *slog.Logger) events.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, e events.Event) error {
		if e.Type != events.TypeHealthDegraded {
			return nil
		}
		reason := e.Reason
		if reason == "" {
			reason = "health event " + e.ID
		}
		if err := s.SignalDegraded(ctx, e.HostID, reason); err != nil {
			log.Warn("failed to signal degraded host", "host", e.HostID, "error", err)
			return err
		}
		log.Info("signalled degraded host", "host", e.HostID, "reason", reason)
		return nil
	}
}
