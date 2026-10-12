package workflows

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

type fixture struct {
	env    *testsuite.TestWorkflowEnvironment
	sim    *hardware.Simulator
	bus    *events.MemoryBus
	hostID string
	in     model.HostLifecycleInput
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	sim := hardware.NewSimulator()
	bus := events.NewMemoryBus()

	ids, err := sim.Reserve(context.Background(), "c1", "batch-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	env.RegisterActivity(&activities.Activities{Provider: sim, Bus: bus})
	env.RegisterWorkflow(HostLifecycleWorkflow)

	return &fixture{env: env, sim: sim, bus: bus, hostID: ids[0], in: model.HostLifecycleInput{
		Spec: model.HostSpec{
			HostID: ids[0], ClusterID: "c1", GPUModel: "H100", GPUCount: 8,
			DriverVersion: "550.90.07", CUDAVersion: "12.4",
		},
	}}
}

func (f *fixture) signalAt(d time.Duration, name string, payload interface{}) {
	f.env.RegisterDelayedCallback(func() { f.env.SignalWorkflow(name, payload) }, d)
}

func (f *fixture) run(t *testing.T) model.HostLifecycleResult {
	t.Helper()
	f.env.ExecuteWorkflow(HostLifecycleWorkflow, f.in)
	if !f.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := f.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var res model.HostLifecycleResult
	if err := f.env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	return res
}

func visited(h domain.Host, s domain.State) bool { return h.Visited(s) }

func TestBringUpThenDecommission(t *testing.T) {
	f := newFixture(t)
	f.signalAt(time.Minute, model.SignalDecommission, model.HostEvent{})
	res := f.run(t)

	if res.FinalState != domain.StateDecommissioned {
		t.Fatalf("final = %s", res.FinalState)
	}
	for _, s := range []domain.State{domain.StateProvisioning, domain.StateDriverInstall, domain.StateValidating, domain.StateReady} {
		if !visited(res.Host, s) {
			t.Errorf("never visited %s", s)
		}
	}
	if len(f.bus.Published()) == 0 {
		t.Error("expected state-change events")
	}
}

func TestTransientFaultsAreRetriedAndBringUpResumes(t *testing.T) {
	f := newFixture(t)
	f.sim.FailNext("InstallGPUStack", 2, false)
	f.signalAt(time.Minute, model.SignalDecommission, model.HostEvent{})
	res := f.run(t)

	if !visited(res.Host, domain.StateReady) {
		t.Fatal("host should still reach READY after transient faults")
	}
	if got := f.sim.Calls("InstallGPUStack"); got != 3 {
		t.Fatalf("InstallGPUStack calls = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestPermanentFaultGoesToRMAWithoutRetry(t *testing.T) {
	f := newFixture(t)
	f.sim.FailNext("InstallGPUStack", 1, true)
	res := f.run(t)

	if !visited(res.Host, domain.StateFailed) || !visited(res.Host, domain.StateRMA) {
		t.Fatalf("expected FAILED and RMA in history: %+v", res.Host.History)
	}
	if res.FinalState != domain.StateDecommissioned {
		t.Fatalf("final = %s", res.FinalState)
	}
	if got := f.sim.Calls("InstallGPUStack"); got != 1 {
		t.Fatalf("permanent failure must not be retried; calls = %d", got)
	}
	if len(f.sim.RMATickets()) != 1 {
		t.Fatalf("RMA tickets = %v", f.sim.RMATickets())
	}
}

func TestSelfHealingRecoversDegradedHost(t *testing.T) {
	f := newFixture(t)
	f.env.RegisterDelayedCallback(func() {
		_ = f.sim.SetHealthy(f.hostID, false)
		f.env.SignalWorkflow(model.SignalDegraded, model.DegradedSignal{Reason: "xid 79"})
	}, time.Minute)
	f.signalAt(5*time.Minute, model.SignalDecommission, model.HostEvent{})
	res := f.run(t)

	for _, s := range []domain.State{domain.StateDegraded, domain.StateDraining, domain.StateRepairing} {
		if !visited(res.Host, s) {
			t.Errorf("never visited %s", s)
		}
	}
	if f.sim.Calls("Repair") != 1 {
		t.Fatalf("repair calls = %d, want 1", f.sim.Calls("Repair"))
	}
	if len(f.sim.RMATickets()) != 0 {
		t.Fatal("recoverable host must not be RMA'd")
	}
}

func TestUnrepairableHostIsRetiredToRMA(t *testing.T) {
	f := newFixture(t)
	f.env.RegisterDelayedCallback(func() {
		_ = f.sim.SetBroken(f.hostID)
		f.env.SignalWorkflow(model.SignalDegraded, model.DegradedSignal{Reason: "dead GPU"})
	}, time.Minute)
	res := f.run(t)

	if !visited(res.Host, domain.StateRMA) || res.FinalState != domain.StateDecommissioned {
		t.Fatalf("final=%s history=%+v", res.FinalState, res.Host.History)
	}
	if got := f.sim.Calls("Repair"); got != maxRepairAttempts {
		t.Fatalf("repair attempts = %d, want %d", got, maxRepairAttempts)
	}
}

func TestDriftTimerReconcilesDriverVersion(t *testing.T) {
	f := newFixture(t)
	f.env.RegisterDelayedCallback(func() { _ = f.sim.SetDriver(f.hostID, "535.00") }, time.Minute)
	f.signalAt(driftInterval+time.Minute, model.SignalDecommission, model.HostEvent{})
	res := f.run(t)

	if !visited(res.Host, domain.StateRepairing) {
		t.Fatalf("drift should trigger remediation: %+v", res.Host.History)
	}
}
