package remediation

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
)

type fakeSignaler struct {
	hosts   []string
	reasons []string
	err     error
}

func (f *fakeSignaler) SignalDegraded(_ context.Context, host, reason string) error {
	f.hosts = append(f.hosts, host)
	f.reasons = append(f.reasons, reason)
	return f.err
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestHandlerSignalsOnlyDegradedEvents(t *testing.T) {
	f := &fakeSignaler{}
	h := Handler(f, quiet)
	ctx := context.Background()

	_ = h(ctx, events.Event{Type: events.TypeStateChanged, HostID: "h0"})
	_ = h(ctx, events.Event{Type: events.TypeHealthDegraded, HostID: "h1", Reason: "xid 79"})
	_ = h(ctx, events.Event{Type: events.TypeHealthDegraded, HostID: "h2", ID: "e2"})

	if len(f.hosts) != 2 || f.hosts[0] != "h1" || f.hosts[1] != "h2" {
		t.Fatalf("hosts = %v", f.hosts)
	}
	if f.reasons[0] != "xid 79" || f.reasons[1] != "health event e2" {
		t.Fatalf("reasons = %v", f.reasons)
	}
}

func TestHandlerPropagatesSignalErrorsForRedelivery(t *testing.T) {
	boom := errors.New("boom")
	h := Handler(&fakeSignaler{err: boom}, quiet)
	err := h(context.Background(), events.Event{Type: events.TypeHealthDegraded, HostID: "h1"})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestWiredToBus(t *testing.T) {
	f := &fakeSignaler{}
	bus := events.NewMemoryBus()
	bus.On(Handler(f, quiet))
	_ = bus.Publish(context.Background(), events.Event{Type: events.TypeHealthDegraded, HostID: "h9", Reason: "ecc"})
	if len(f.hosts) != 1 || f.hosts[0] != "h9" {
		t.Fatalf("hosts = %v", f.hosts)
	}
}
