package events

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryBusFanOutAndRecording(t *testing.T) {
	b := NewMemoryBus()
	var a, c int
	b.On(func(context.Context, Event) error { a++; return nil })
	b.On(func(context.Context, Event) error { c++; return nil })
	if err := b.Publish(context.Background(), Event{ID: "1", Type: TypeStateChanged, HostID: "h"}); err != nil {
		t.Fatal(err)
	}
	if a != 1 || c != 1 || len(b.Published()) != 1 {
		t.Fatalf("a=%d c=%d published=%d", a, c, len(b.Published()))
	}
}

func TestMemoryBusJoinsHandlerErrors(t *testing.T) {
	b := NewMemoryBus()
	boom := errors.New("boom")
	b.On(func(context.Context, Event) error { return boom })
	b.On(func(context.Context, Event) error { return nil })
	if err := b.Publish(context.Background(), Event{}); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestSubscribeBlocksUntilCancelled(t *testing.T) {
	b := NewMemoryBus()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	got := make(chan Event, 1)
	go func() { done <- b.Subscribe(ctx, func(_ context.Context, e Event) error { got <- e; return nil }) }()

	deadline := time.After(2 * time.Second)
	for {
		_ = b.Publish(context.Background(), Event{ID: "x"})
		select {
		case <-got:
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return
		case <-deadline:
			t.Fatal("handler never invoked")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestNewFromEnvDefaultsToMemory(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	b, err := NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := b.(*MemoryBus); !ok {
		t.Fatalf("got %T, want *MemoryBus", b)
	}
}
