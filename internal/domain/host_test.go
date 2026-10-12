package domain

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func TestHappyPathAndDecommission(t *testing.T) {
	h := NewHost("h1", "c1", "H100", 8, t0)
	path := []State{StateProvisioning, StateDriverInstall, StateValidating, StateReady, StateDecommissioning, StateDecommissioned}
	for i, s := range path {
		if err := h.Transition(s, "step", t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("transition to %s: %v", s, err)
		}
	}
	if h.State != StateDecommissioned || !IsTerminal(h.State) {
		t.Fatalf("want terminal DECOMMISSIONED, got %s", h.State)
	}
	if h.Version != int64(1+len(path)) {
		t.Fatalf("version = %d, want %d", h.Version, 1+len(path))
	}
	if len(h.History) != len(path) {
		t.Fatalf("history len = %d, want %d", len(h.History), len(path))
	}
}

func TestSelfHealingLoop(t *testing.T) {
	h := NewHost("h1", "c1", "H100", 8, t0)
	for _, s := range []State{StateProvisioning, StateDriverInstall, StateValidating, StateReady,
		StateDegraded, StateDraining, StateRepairing, StateValidating, StateReady} {
		if err := h.Transition(s, "x", t0); err != nil {
			t.Fatalf("transition to %s: %v", s, err)
		}
	}
	if !h.Visited(StateRepairing) {
		t.Fatal("expected REPAIRING in history")
	}
}

func TestInvalidTransitions(t *testing.T) {
	cases := []struct{ from, to State }{
		{StateDiscovered, StateReady},
		{StateReady, StateRepairing},
		{StateReady, StateRMA},
		{StateDecommissioned, StateReady},
		{StateFailed, StateReady},
	}
	for _, c := range cases {
		h := Host{State: c.from}
		err := h.Transition(c.to, "x", t0)
		var inv *ErrInvalidTransition
		if !errors.As(err, &inv) {
			t.Errorf("%s -> %s: want ErrInvalidTransition, got %v", c.from, c.to, err)
		}
		if h.State != c.from || h.Version != 0 {
			t.Errorf("%s -> %s: host mutated on failed transition", c.from, c.to)
		}
	}
}

func TestEveryNonTerminalStateHasAnExit(t *testing.T) {
	for s, next := range allowed {
		if s == StateDecommissioned {
			continue
		}
		if len(next) == 0 {
			t.Errorf("state %s has no outgoing transitions", s)
		}
	}
}

func TestEveryStateReachesDecommissioned(t *testing.T) {
	for start := range allowed {
		seen := map[State]bool{}
		var walk func(State) bool
		walk = func(s State) bool {
			if s == StateDecommissioned {
				return true
			}
			if seen[s] {
				return false
			}
			seen[s] = true
			for _, n := range allowed[s] {
				if walk(n) {
					return true
				}
			}
			return false
		}
		if !walk(start) {
			t.Errorf("state %s cannot reach DECOMMISSIONED", start)
		}
	}
}

func TestHistoryIsBounded(t *testing.T) {
	h := NewHost("h1", "c1", "H100", 8, t0)
	_ = h.Transition(StateProvisioning, "", t0)
	_ = h.Transition(StateDriverInstall, "", t0)
	_ = h.Transition(StateValidating, "", t0)
	_ = h.Transition(StateReady, "", t0)
	for i := 0; i < 200; i++ {
		_ = h.Transition(StateDegraded, "", t0)
		_ = h.Transition(StateDraining, "", t0)
		_ = h.Transition(StateRepairing, "", t0)
		_ = h.Transition(StateValidating, "", t0)
		_ = h.Transition(StateReady, "", t0)
	}
	if len(h.History) != maxHistory {
		t.Fatalf("history len = %d, want %d", len(h.History), maxHistory)
	}
}
