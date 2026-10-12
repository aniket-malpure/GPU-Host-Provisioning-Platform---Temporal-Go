package workflows

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

func clusterEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, *hardware.Simulator) {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	sim := hardware.NewSimulator()
	env.RegisterActivity(&activities.Activities{Provider: sim, Bus: events.NewMemoryBus()})
	env.RegisterWorkflow(ClusterWorkflow)
	env.RegisterWorkflow(HostLifecycleWorkflow)
	return env, sim
}

func clusterInput(gpus int) model.ClusterInput {
	m := model.Manifest{Metadata: model.Metadata{Name: "llama-prod"}, Spec: model.ClusterSpec{GPUs: gpus}}.Normalize()
	return model.ClusterInput{ClusterID: "cluster-llama-prod", Name: "llama-prod", Spec: m.Spec}
}

func queryStatus(t *testing.T, env *testsuite.TestWorkflowEnvironment) model.ClusterStatus {
	t.Helper()
	v, err := env.QueryWorkflow(model.QueryCluster)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var st model.ClusterStatus
	if err := v.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestClusterProvisionsScalesAndTearsDown(t *testing.T) {
	env, _ := clusterEnv(t)
	var atReady, atScaledUp model.ClusterStatus

	env.RegisterDelayedCallback(func() { atReady = queryStatus(t, env) }, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(model.SignalScale, model.ScaleSignal{DeltaHosts: 1}) }, 2*time.Minute)
	env.RegisterDelayedCallback(func() { atScaledUp = queryStatus(t, env) }, 3*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(model.SignalTeardown, nil) }, 4*time.Minute)

	env.ExecuteWorkflow(ClusterWorkflow, clusterInput(16)) // 2 hosts
	if !env.IsWorkflowCompleted() {
		t.Fatal("cluster workflow did not complete after teardown")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	if atReady.Phase != model.PhaseReady || atReady.ReadyHosts != 2 {
		t.Fatalf("after create: %+v", atReady)
	}
	if atScaledUp.DesiredHosts != 3 || atScaledUp.ReadyHosts != 3 {
		t.Fatalf("after scale-up: %+v", atScaledUp)
	}
	var final model.ClusterStatus
	if err := env.GetWorkflowResult(&final); err != nil {
		t.Fatal(err)
	}
	if final.Phase != model.PhaseTerminated {
		t.Fatalf("final phase = %s", final.Phase)
	}
}

func TestClusterReplacesHostThatFailsBringUp(t *testing.T) {
	env, sim := clusterEnv(t)
	sim.FailNext("Discover", 1, true) // first host's hardware is dead on arrival

	var st model.ClusterStatus
	env.RegisterDelayedCallback(func() { st = queryStatus(t, env) }, 2*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(model.SignalTeardown, nil) }, 3*time.Minute)
	env.ExecuteWorkflow(ClusterWorkflow, clusterInput(16))

	if st.Phase != model.PhaseReady || st.ReadyHosts != 2 || st.FailedHosts != 1 {
		t.Fatalf("want Ready with 2 ready hosts and 1 failed (replaced), got %+v", st)
	}
	if len(sim.RMATickets()) != 1 {
		t.Fatalf("RMA tickets = %v", sim.RMATickets())
	}
}
