// Package activities contains the side-effecting steps Temporal workflows
// invoke. They are plain Go methods (no SDK import) so they can be unit tested
// directly. Every activity is idempotent: Temporal retries them freely.
package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/drift"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
)

// DefaultOSImage is installed during PXE bring-up.
const DefaultOSImage = "ubuntu-24.04-gpu"

// Activities is registered with a Temporal worker.
type Activities struct {
	Provider hardware.Provider
	Bus      events.Bus
	OSImage  string
}

// Inputs.
type (
	HostRef      struct{ HostID string }
	ReserveInput struct {
		ClusterID string
		Key       string // idempotency key, e.g. "initial", "scale-3"
		Count     int
	}
	StackInput struct {
		HostID string
		Driver string
		CUDA   string
	}
	DriftInput struct {
		HostID  string
		Desired drift.Desired
	}
	RMAInput struct {
		HostID string
		Reason string
	}
	StateChange struct {
		HostID    string
		ClusterID string
		From      string
		To        string
		Reason    string
		Version   int64
		At        time.Time
	}
)

func (a *Activities) image() string {
	if a.OSImage != "" {
		return a.OSImage
	}
	return DefaultOSImage
}

// ReserveHosts allocates n hosts from the free pool for a cluster.
func (a *Activities) ReserveHosts(ctx context.Context, in ReserveInput) ([]string, error) {
	return a.Provider.Reserve(ctx, in.ClusterID, in.Key, in.Count)
}

// DiscoverHardware inventories a host through its BMC.
func (a *Activities) DiscoverHardware(ctx context.Context, in HostRef) (hardware.Inventory, error) {
	return a.Provider.Discover(ctx, in.HostID)
}

// ProvisionOS power-cycles the host and network-boots the OS image.
func (a *Activities) ProvisionOS(ctx context.Context, in HostRef) error {
	if err := a.Provider.PowerCycle(ctx, in.HostID); err != nil {
		return err
	}
	return a.Provider.InstallOS(ctx, in.HostID, a.image())
}

// InstallGPUStack converges the host to the requested driver and CUDA version.
func (a *Activities) InstallGPUStack(ctx context.Context, in StackInput) error {
	return a.Provider.InstallGPUStack(ctx, in.HostID, in.Driver, in.CUDA)
}

// ValidateHealth runs the health suite. An unhealthy host is a normal result,
// not an error; the workflow decides what to do with it.
func (a *Activities) ValidateHealth(ctx context.Context, in HostRef) (hardware.HealthReport, error) {
	return a.Provider.RunHealthCheck(ctx, in.HostID)
}

// CordonAndDrain stops new scheduling and evicts running workloads.
func (a *Activities) CordonAndDrain(ctx context.Context, in HostRef) error {
	if err := a.Provider.Cordon(ctx, in.HostID); err != nil {
		return err
	}
	return a.Provider.Drain(ctx, in.HostID)
}

// RepairHost resets the host's GPUs and BMC state.
func (a *Activities) RepairHost(ctx context.Context, in HostRef) error {
	return a.Provider.Repair(ctx, in.HostID)
}

// Uncordon returns a healthy host to the schedulable pool.
func (a *Activities) Uncordon(ctx context.Context, in HostRef) error {
	return a.Provider.Uncordon(ctx, in.HostID)
}

// DetectDrift compares observed state with desired configuration.
func (a *Activities) DetectDrift(ctx context.Context, in DriftInput) ([]drift.Drift, error) {
	obs, err := a.Provider.Inspect(ctx, in.HostID)
	if err != nil {
		return nil, err
	}
	return drift.Detect(in.Desired, obs), nil
}

// DecommissionHost safely drains, powers off and wipes a host.
func (a *Activities) DecommissionHost(ctx context.Context, in HostRef) error {
	steps := []func(context.Context, string) error{
		a.Provider.Cordon, a.Provider.Drain, a.Provider.PowerOff, a.Provider.Wipe,
	}
	for _, step := range steps {
		if err := step(ctx, in.HostID); err != nil {
			return err
		}
	}
	return nil
}

// OpenRMA opens a vendor return ticket and returns its ID.
func (a *Activities) OpenRMA(ctx context.Context, in RMAInput) (string, error) {
	return a.Provider.OpenRMA(ctx, in.HostID, in.Reason)
}

// PublishStateChange emits a host.state.changed event. The event ID is
// hostID:version, so at-least-once delivery is safely de-duplicable.
func (a *Activities) PublishStateChange(ctx context.Context, in StateChange) error {
	if a.Bus == nil {
		return nil
	}
	return a.Bus.Publish(ctx, events.Event{
		ID:        fmt.Sprintf("%s:%d", in.HostID, in.Version),
		Type:      events.TypeStateChanged,
		HostID:    in.HostID,
		ClusterID: in.ClusterID,
		From:      in.From,
		To:        in.To,
		Reason:    in.Reason,
		Time:      in.At,
	})
}
