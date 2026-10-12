// Package hardware defines the interface to physical infrastructure (BMC,
// PXE, GPU stack, scheduler drain) and a simulator used for development, tests
// and the docker-compose demo. A production implementation would talk to
// Redfish/IPMI, an iPXE/MAAS service, Ansible-free node agents and the
// Kubernetes API behind the same interface.
package hardware

import (
	"context"
	"errors"
	"fmt"
)

// Provider abstracts every side effect the provisioning workflows perform.
// All methods MUST be idempotent: Temporal may retry any activity, and a
// retried call after a successful-but-unacknowledged attempt must be a no-op.
type Provider interface {
	Reserve(ctx context.Context, clusterID, key string, n int) ([]string, error)
	Discover(ctx context.Context, hostID string) (Inventory, error)
	PowerCycle(ctx context.Context, hostID string) error
	InstallOS(ctx context.Context, hostID, image string) error
	InstallGPUStack(ctx context.Context, hostID, driver, cuda string) error
	Inspect(ctx context.Context, hostID string) (Observed, error)
	RunHealthCheck(ctx context.Context, hostID string) (HealthReport, error)
	Cordon(ctx context.Context, hostID string) error
	Drain(ctx context.Context, hostID string) error
	Uncordon(ctx context.Context, hostID string) error
	Repair(ctx context.Context, hostID string) error
	PowerOff(ctx context.Context, hostID string) error
	Wipe(ctx context.Context, hostID string) error
	OpenRMA(ctx context.Context, hostID, reason string) (ticket string, err error)
}

// Inventory is what discovery reports about a host.
type Inventory struct {
	Model    string `json:"model"`
	GPUCount int    `json:"gpuCount"`
	Serial   string `json:"serial"`
}

// Observed is the live state used for drift detection.
type Observed struct {
	DriverVersion string `json:"driverVersion"`
	CUDAVersion   string `json:"cudaVersion"`
	GPUCount      int    `json:"gpuCount"`
	OSImage       string `json:"osImage"`
	PoweredOn     bool   `json:"poweredOn"`
	Cordoned      bool   `json:"cordoned"`
}

// Check is a single health probe result.
type Check struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail,omitempty"`
}

// HealthReport aggregates health probes (ECC, NVLink, DCGM, NCCL, ...).
type HealthReport struct {
	Healthy bool    `json:"healthy"`
	Checks  []Check `json:"checks"`
}

// PermanentErrorType is the Temporal error type name used in retry policies.
// Temporal derives the type of a non-Temporal error from the Go type name, so
// *PermanentError surfaces as "PermanentError" (asserted in tests).
const PermanentErrorType = "PermanentError"

// PermanentError marks a failure that retrying cannot fix (dead GPU, unknown
// host, bad firmware). Workflows route it to the RMA path.
type PermanentError struct {
	Op     string
	Reason string
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent failure in %s: %s", e.Op, e.Reason)
}

// IsPermanent reports whether err is, or wraps, a *PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}
