//go:build !kafka

package events

func newFromEnv() (Bus, error) { return NewMemoryBus(), nil }
