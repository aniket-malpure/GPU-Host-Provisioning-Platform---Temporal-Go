// Package model contains the API manifest and the data types shared by the API
// server, the orchestrator client and the Temporal workflows. It depends only
// on the standard library and the domain package so every consumer (including
// tests) can use it without pulling in the Temporal SDK.
package model

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// Workflow signal and query names (the public contract with running workflows).
const (
	SignalDegraded     = "degraded"
	SignalDecommission = "decommission"
	SignalHostReady    = "host-ready"
	SignalHostFailed   = "host-failed"
	SignalHostGone     = "host-gone"
	SignalScale        = "scale"
	SignalTeardown     = "teardown"

	QueryHost    = "host"
	QueryCluster = "cluster"
)

// Cluster phases.
const (
	PhaseProvisioning = "Provisioning"
	PhaseReady        = "Ready"
	PhaseScaling      = "Scaling"
	PhaseDegraded     = "Degraded"
	PhaseTerminating  = "Terminating"
	PhaseTerminated   = "Terminated"
)

const (
	APIVersion           = "gpu.platform/v1"
	KindInferenceCluster = "InferenceCluster"

	DefaultGPUsPerHost = 8
	DefaultDriver      = "550.90.07"
	DefaultCUDA        = "12.4"
)

// Manifest is the declarative request: the inference team states what they
// need; the platform decides how.
type Manifest struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   Metadata    `json:"metadata"`
	Spec       ClusterSpec `json:"spec"`
}

type Metadata struct {
	Name string `json:"name"`
}

type Topology struct {
	Interconnect string `json:"interconnect,omitempty"` // ethernet | infiniband | roce
	Rack         string `json:"rack,omitempty"`
}

type ClusterSpec struct {
	GPUModel      string   `json:"gpuModel"`
	GPUs          int      `json:"gpus"`
	GPUsPerHost   int      `json:"gpusPerHost,omitempty"`
	Topology      Topology `json:"topology,omitempty"`
	DriverVersion string   `json:"driverVersion,omitempty"`
	CUDAVersion   string   `json:"cudaVersion,omitempty"`
}

var nameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,40}[a-z0-9])?$`)

// Normalize returns a copy with defaults applied.
func (m Manifest) Normalize() Manifest {
	if m.APIVersion == "" {
		m.APIVersion = APIVersion
	}
	if m.Kind == "" {
		m.Kind = KindInferenceCluster
	}
	if m.Spec.GPUModel == "" {
		m.Spec.GPUModel = "H100"
	}
	if m.Spec.GPUsPerHost == 0 {
		m.Spec.GPUsPerHost = DefaultGPUsPerHost
	}
	if m.Spec.DriverVersion == "" {
		m.Spec.DriverVersion = DefaultDriver
	}
	if m.Spec.CUDAVersion == "" {
		m.Spec.CUDAVersion = DefaultCUDA
	}
	return m
}

// Validate checks a normalized manifest.
func (m Manifest) Validate() error {
	if m.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if m.Kind != KindInferenceCluster {
		return fmt.Errorf("kind must be %q", KindInferenceCluster)
	}
	if !nameRe.MatchString(m.Metadata.Name) {
		return errors.New("metadata.name must be a lowercase DNS-style label (max 42 chars)")
	}
	s := m.Spec
	switch s.GPUsPerHost {
	case 1, 2, 4, 8:
	default:
		return errors.New("spec.gpusPerHost must be one of 1, 2, 4, 8")
	}
	if s.GPUs <= 0 || s.GPUs > 4096 {
		return errors.New("spec.gpus must be between 1 and 4096")
	}
	if s.GPUs%s.GPUsPerHost != 0 {
		return fmt.Errorf("spec.gpus (%d) must be a multiple of spec.gpusPerHost (%d)", s.GPUs, s.GPUsPerHost)
	}
	switch s.Topology.Interconnect {
	case "", "ethernet", "infiniband", "roce":
	default:
		return errors.New("spec.topology.interconnect must be ethernet, infiniband or roce")
	}
	return nil
}

// Hosts is the number of physical hosts the spec requires.
func (s ClusterSpec) Hosts() int {
	if s.GPUsPerHost <= 0 {
		return 0
	}
	return (s.GPUs + s.GPUsPerHost - 1) / s.GPUsPerHost
}

// ImmutableDiff lists fields that differ between two specs other than the
// GPU count, which is the only field that may change after creation.
func ImmutableDiff(old, new ClusterSpec) []string {
	var out []string
	if old.GPUModel != new.GPUModel {
		out = append(out, "spec.gpuModel")
	}
	if old.GPUsPerHost != new.GPUsPerHost {
		out = append(out, "spec.gpusPerHost")
	}
	if old.Topology != new.Topology {
		out = append(out, "spec.topology")
	}
	if old.DriverVersion != new.DriverVersion {
		out = append(out, "spec.driverVersion")
	}
	if old.CUDAVersion != new.CUDAVersion {
		out = append(out, "spec.cudaVersion")
	}
	return out
}

// ClusterInput starts a ClusterWorkflow.
type ClusterInput struct {
	ClusterID string      `json:"clusterId"`
	Name      string      `json:"name"`
	Spec      ClusterSpec `json:"spec"`
}

// HostSummary is one host inside a cluster status.
type HostSummary struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// ClusterStatus is returned by the cluster workflow's query handler.
type ClusterStatus struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	Phase        string        `json:"phase"`
	Spec         ClusterSpec   `json:"spec"`
	DesiredHosts int           `json:"desiredHosts"`
	ReadyHosts   int           `json:"readyHosts"`
	FailedHosts  int           `json:"failedHosts"`
	Hosts        []HostSummary `json:"hosts"`
}

// HostSpec describes one host to bring up.
type HostSpec struct {
	HostID        string `json:"hostId"`
	ClusterID     string `json:"clusterId"`
	GPUModel      string `json:"gpuModel"`
	GPUCount      int    `json:"gpuCount"`
	Rack          string `json:"rack,omitempty"`
	DriverVersion string `json:"driverVersion"`
	CUDAVersion   string `json:"cudaVersion"`
}

// HostLifecycleInput starts (or continues-as-new) a HostLifecycleWorkflow.
type HostLifecycleInput struct {
	Spec             HostSpec    `json:"spec"`
	ParentWorkflowID string      `json:"parentWorkflowId,omitempty"`
	Resume           bool        `json:"resume,omitempty"`
	Host             domain.Host `json:"host,omitempty"`
	RepairAttempts   int         `json:"repairAttempts,omitempty"`
}

// HostLifecycleResult is the workflow's final output.
type HostLifecycleResult struct {
	HostID     string       `json:"hostId"`
	FinalState domain.State `json:"finalState"`
	Reason     string       `json:"reason"`
	Host       domain.Host  `json:"host"`
}

// Signal payloads.
type DegradedSignal struct {
	Reason string `json:"reason"`
}

type HostEvent struct {
	HostID string `json:"hostId"`
	Reason string `json:"reason,omitempty"`
}

type ScaleSignal struct {
	DeltaHosts int `json:"deltaHosts"`
}
