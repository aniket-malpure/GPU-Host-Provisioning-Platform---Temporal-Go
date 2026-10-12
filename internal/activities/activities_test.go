package activities

import (
	"context"
	"testing"
	"time"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/drift"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
)

var ctx = context.Background()

func setup(t *testing.T) (*Activities, *hardware.Simulator, *events.MemoryBus, string) {
	t.Helper()
	sim := hardware.NewSimulator()
	bus := events.NewMemoryBus()
	a := &Activities{Provider: sim, Bus: bus}
	ids, err := a.ReserveHosts(ctx, ReserveInput{ClusterID: "c1", Key: "initial", Count: 1})
	if err != nil || len(ids) != 1 {
		t.Fatalf("reserve: %v %v", ids, err)
	}
	return a, sim, bus, ids[0]
}

func bringUp(t *testing.T, a *Activities, id string) {
	t.Helper()
	if _, err := a.DiscoverHardware(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
	if err := a.ProvisionOS(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
	if err := a.InstallGPUStack(ctx, StackInput{id, "550.90", "12.4"}); err != nil {
		t.Fatal(err)
	}
}

func TestBringUpAndValidate(t *testing.T) {
	a, _, _, id := setup(t)
	bringUp(t, a, id)
	rep, err := a.ValidateHealth(ctx, HostRef{id})
	if err != nil || !rep.Healthy {
		t.Fatalf("want healthy, got %+v err=%v", rep, err)
	}
}

func TestActivitiesAreSafeToRetry(t *testing.T) {
	a, _, _, id := setup(t)
	bringUp(t, a, id)
	bringUp(t, a, id) // simulate Temporal re-running the whole sequence
	if rep, _ := a.ValidateHealth(ctx, HostRef{id}); !rep.Healthy {
		t.Fatal("host should remain healthy after repeated activities")
	}
}

func TestDriftDetectionAndConvergence(t *testing.T) {
	a, sim, _, id := setup(t)
	bringUp(t, a, id)
	want := drift.Desired{DriverVersion: "550.90", CUDAVersion: "12.4", GPUCount: 8}

	ds, err := a.DetectDrift(ctx, DriftInput{id, want})
	if err != nil || len(ds) != 0 {
		t.Fatalf("unexpected drift %v err=%v", ds, err)
	}
	_ = sim.SetDriver(id, "535.00")
	ds, _ = a.DetectDrift(ctx, DriftInput{id, want})
	if len(ds) != 1 || ds[0].Field != "driverVersion" {
		t.Fatalf("want driver drift, got %v", ds)
	}
	_ = a.InstallGPUStack(ctx, StackInput{id, "550.90", "12.4"}) // reconcile
	if ds, _ = a.DetectDrift(ctx, DriftInput{id, want}); len(ds) != 0 {
		t.Fatalf("drift should be resolved, got %v", ds)
	}
}

func TestSelfHealSequence(t *testing.T) {
	a, sim, _, id := setup(t)
	bringUp(t, a, id)
	_ = sim.SetHealthy(id, false)
	if rep, _ := a.ValidateHealth(ctx, HostRef{id}); rep.Healthy {
		t.Fatal("expected unhealthy")
	}
	if err := a.CordonAndDrain(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
	if err := a.RepairHost(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
	if rep, _ := a.ValidateHealth(ctx, HostRef{id}); !rep.Healthy {
		t.Fatal("expected healthy after repair")
	}
	if err := a.Uncordon(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
}

func TestDecommissionPowersOffAndWipes(t *testing.T) {
	a, sim, _, id := setup(t)
	bringUp(t, a, id)
	if err := a.DecommissionHost(ctx, HostRef{id}); err != nil {
		t.Fatal(err)
	}
	obs, _ := sim.Inspect(ctx, id)
	if obs.PoweredOn || obs.OSImage != "" || obs.DriverVersion != "" {
		t.Fatalf("host not clean: %+v", obs)
	}
	if err := a.DecommissionHost(ctx, HostRef{id}); err != nil { // idempotent
		t.Fatal(err)
	}
}

func TestPublishStateChangeUsesDeterministicID(t *testing.T) {
	a, _, bus, id := setup(t)
	sc := StateChange{HostID: id, ClusterID: "c1", From: "A", To: "B", Version: 7, At: time.Unix(0, 0)}
	_ = a.PublishStateChange(ctx, sc)
	_ = a.PublishStateChange(ctx, sc)
	evs := bus.Published()
	if len(evs) != 2 || evs[0].ID != evs[1].ID || evs[0].ID != id+":7" {
		t.Fatalf("events = %+v", evs)
	}
	if evs[0].Type != events.TypeStateChanged {
		t.Fatalf("type = %s", evs[0].Type)
	}
}

func TestPermanentHardwareFaultSurfacesAsPermanent(t *testing.T) {
	a, sim, _, id := setup(t)
	sim.FailNext("Discover", 1, true)
	_, err := a.DiscoverHardware(ctx, HostRef{id})
	if !hardware.IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
}
