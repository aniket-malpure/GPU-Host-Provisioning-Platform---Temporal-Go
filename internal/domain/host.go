// Package domain holds the pure, deterministic host lifecycle model.
//
// It has no I/O and no dependency on Temporal, so it is safe to call from
// workflow code and trivial to unit test. States and transitions are explicit
// and versioned: SchemaVersion is bumped whenever the transition table changes
// in a way that affects in-flight hosts.
package domain

import (
	"fmt"
	"time"
)

// State is a lifecycle state of a physical GPU host.
type State string

const (
	StateDiscovered      State = "DISCOVERED"
	StateProvisioning    State = "PROVISIONING"   // power cycle + PXE/OS install
	StateDriverInstall   State = "DRIVER_INSTALL" // GPU driver + CUDA stack
	StateValidating      State = "VALIDATING"     // health validation
	StateReady           State = "READY"          // serving capacity
	StateDegraded        State = "DEGRADED"       // health signal or drift detected
	StateDraining        State = "DRAINING"       // workloads being moved off
	StateRepairing       State = "REPAIRING"      // reset / reinstall
	StateFailed          State = "FAILED"         // bring-up failed
	StateDecommissioning State = "DECOMMISSIONING"
	StateRMA             State = "RMA"            // returned to vendor
	StateDecommissioned  State = "DECOMMISSIONED" // terminal
)

// SchemaVersion identifies the transition table below.
const SchemaVersion = 1

// maxHistory bounds the history carried in workflow state / ContinueAsNew.
const maxHistory = 50

var allowed = map[State][]State{
	StateDiscovered:      {StateProvisioning, StateFailed},
	StateProvisioning:    {StateDriverInstall, StateFailed},
	StateDriverInstall:   {StateValidating, StateFailed},
	StateValidating:      {StateReady, StateDegraded, StateFailed, StateRMA},
	StateReady:           {StateDegraded, StateDecommissioning},
	StateDegraded:        {StateDraining, StateRMA},
	StateDraining:        {StateRepairing, StateDecommissioning, StateRMA},
	StateRepairing:       {StateValidating, StateRMA},
	StateFailed:          {StateDecommissioning, StateRMA},
	StateDecommissioning: {StateDecommissioned, StateRMA},
	StateRMA:             {StateDecommissioned},
	StateDecommissioned:  {},
}

// CanTransition reports whether from -> to is a legal edge.
func CanTransition(from, to State) bool {
	for _, s := range allowed[from] {
		if s == to {
			return true
		}
	}
	return false
}

// IsTerminal reports whether no further transitions are possible.
func IsTerminal(s State) bool { return len(allowed[s]) == 0 }

// ErrInvalidTransition is returned for an illegal edge.
type ErrInvalidTransition struct{ From, To State }

func (e *ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid host transition %s -> %s", e.From, e.To)
}

// Transition is one recorded state change.
type Transition struct {
	From    State     `json:"from"`
	To      State     `json:"to"`
	Reason  string    `json:"reason"`
	Version int64     `json:"version"`
	At      time.Time `json:"at"`
}

// Host is the state-machine record for one physical host.
type Host struct {
	ID            string       `json:"id"`
	ClusterID     string       `json:"clusterId,omitempty"`
	GPUModel      string       `json:"gpuModel,omitempty"`
	GPUCount      int          `json:"gpuCount"`
	Rack          string       `json:"rack,omitempty"`
	DesiredDriver string       `json:"desiredDriver,omitempty"`
	DesiredCUDA   string       `json:"desiredCuda,omitempty"`
	State         State        `json:"state"`
	Version       int64        `json:"version"`
	SchemaVersion int          `json:"schemaVersion"`
	UpdatedAt     time.Time    `json:"updatedAt"`
	History       []Transition `json:"history,omitempty"`
}

// NewHost returns a host in the DISCOVERED state at version 1.
func NewHost(id, clusterID, gpuModel string, gpuCount int, now time.Time) Host {
	return Host{
		ID:            id,
		ClusterID:     clusterID,
		GPUModel:      gpuModel,
		GPUCount:      gpuCount,
		State:         StateDiscovered,
		Version:       1,
		SchemaVersion: SchemaVersion,
		UpdatedAt:     now,
	}
}

// Transition moves the host to a new state, recording history and bumping the
// version. now must be supplied by the caller (workflow.Now in workflows) so
// the function stays deterministic.
func (h *Host) Transition(to State, reason string, now time.Time) error {
	if !CanTransition(h.State, to) {
		return &ErrInvalidTransition{From: h.State, To: to}
	}
	h.Version++
	h.History = append(h.History, Transition{From: h.State, To: to, Reason: reason, Version: h.Version, At: now})
	if len(h.History) > maxHistory {
		h.History = append([]Transition(nil), h.History[len(h.History)-maxHistory:]...)
	}
	h.State = to
	h.UpdatedAt = now
	return nil
}

// Visited reports whether the host has ever entered the given state (within
// the retained history).
func (h *Host) Visited(s State) bool {
	for _, t := range h.History {
		if t.To == s {
			return true
		}
	}
	return false
}
