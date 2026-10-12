// Package events defines the platform event stream. Health, remediation and
// utilization systems are event-driven: producers publish, consumers react,
// nothing polls. Kafka is the production transport (build tag `kafka`); an
// in-process bus is the default for tests and single-process demos.
package events

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Type names an event kind.
type Type string

const (
	TypeStateChanged   Type = "host.state.changed"
	TypeHealthDegraded Type = "host.health.degraded"
)

// Event is the wire format. ID is deterministic for state changes
// (hostID:version) so consumers can de-duplicate at-least-once delivery.
type Event struct {
	ID        string    `json:"id"`
	Type      Type      `json:"type"`
	HostID    string    `json:"hostId"`
	ClusterID string    `json:"clusterId,omitempty"`
	From      string    `json:"from,omitempty"`
	To        string    `json:"to,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	Time      time.Time `json:"time"`
}

// Handler processes one event. Returning an error requests redelivery where
// the transport supports it.
type Handler func(ctx context.Context, e Event) error

// Bus is the publish/subscribe abstraction.
type Bus interface {
	Publish(ctx context.Context, e Event) error
	// Subscribe delivers events to h until ctx is cancelled.
	Subscribe(ctx context.Context, h Handler) error
	Close() error
}

// NewFromEnv builds the transport selected at compile time: Kafka when built
// with `-tags kafka` and KAFKA_BROKERS is set, otherwise an in-process bus.
func NewFromEnv() (Bus, error) { return newFromEnv() }

// MemoryBus is a synchronous in-process Bus that also records everything
// published, which makes assertions in tests trivial.
type MemoryBus struct {
	mu        sync.Mutex
	handlers  []Handler
	published []Event
}

// NewMemoryBus returns an empty bus.
func NewMemoryBus() *MemoryBus { return &MemoryBus{} }

var _ Bus = (*MemoryBus)(nil)

// On registers a handler without blocking.
func (b *MemoryBus) On(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
}

// Publish records the event and invokes every handler synchronously.
func (b *MemoryBus) Publish(ctx context.Context, e Event) error {
	b.mu.Lock()
	b.published = append(b.published, e)
	hs := append([]Handler(nil), b.handlers...)
	b.mu.Unlock()
	var errs []error
	for _, h := range hs {
		if err := h(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Subscribe registers h and blocks until ctx is done.
func (b *MemoryBus) Subscribe(ctx context.Context, h Handler) error {
	b.On(h)
	<-ctx.Done()
	return nil
}

// Published returns a copy of everything published so far.
func (b *MemoryBus) Published() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Event(nil), b.published...)
}

// Close is a no-op.
func (b *MemoryBus) Close() error { return nil }
