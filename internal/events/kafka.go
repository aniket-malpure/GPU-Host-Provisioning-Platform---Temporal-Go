//go:build kafka

package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaBus publishes and consumes events on a single topic. Messages are keyed
// by host ID so all events for one host land on one partition, in order.
// Delivery is at-least-once; consumers de-duplicate on Event.ID.
type KafkaBus struct {
	brokers []string
	topic   string
	group   string
	w       *kafka.Writer
}

var _ Bus = (*KafkaBus)(nil)

// NewKafkaBus returns a Kafka-backed Bus.
func NewKafkaBus(brokers []string, topic, group string) *KafkaBus {
	return &KafkaBus{
		brokers: brokers, topic: topic, group: group,
		w: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

func newFromEnv() (Bus, error) {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return NewMemoryBus(), nil
	}
	topic := getenv("KAFKA_TOPIC", "gpu.host.events")
	group := getenv("KAFKA_GROUP", "gpu-remediator")
	return NewKafkaBus(strings.Split(raw, ","), topic, group), nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func (k *KafkaBus) Publish(ctx context.Context, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return k.w.WriteMessages(ctx, kafka.Message{Key: []byte(e.HostID), Value: b})
}

func (k *KafkaBus) Subscribe(ctx context.Context, h Handler) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.brokers, GroupID: k.group, Topic: k.topic,
		MinBytes: 1, MaxBytes: 1 << 20, StartOffset: kafka.LastOffset,
	})
	defer r.Close()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var e Event
		if err := json.Unmarshal(m.Value, &e); err != nil {
			slog.Warn("dropping malformed event", "offset", m.Offset, "error", err)
			_ = r.CommitMessages(ctx, m)
			continue
		}
		var herr error
		for attempt := 0; attempt < 3; attempt++ {
			if herr = h(ctx, e); herr == nil {
				break
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Duration(attempt+1) * time.Second):
			}
		}
		if herr != nil {
			slog.Error("handler failed after retries; skipping event", "event", e.ID, "error", herr)
		}
		if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			return err
		}
	}
}

func (k *KafkaBus) Close() error { return k.w.Close() }
