package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

const (
	hostProvisioning   = "provisioning"
	hostReady          = "ready"
	hostDecommissioned = "decommissioning"
	hostFailed         = "failed"
	hostGone           = "gone"
)

type hostEntry struct{ ID, State string }

type pendingEvent struct {
	kind   string
	hostID string
	delta  int
}

// ClusterWorkflow reconciles one InferenceCluster manifest: it reserves the
// hosts, starts one HostLifecycleWorkflow per host, waits for them via
// signals (no polling), replaces hosts that fail, and handles scale and
// teardown requests. Its status is served through a query handler.
func ClusterWorkflow(ctx workflow.Context, in model.ClusterInput) (model.ClusterStatus, error) {
	c := &clusterRun{ctx: ctx, in: in, phase: model.PhaseProvisioning, desired: in.Spec.Hosts()}
	if err := workflow.SetQueryHandler(ctx, model.QueryCluster, func() (model.ClusterStatus, error) { return c.status(), nil }); err != nil {
		return model.ClusterStatus{}, err
	}
	return c.run()
}

type clusterRun struct {
	ctx          workflow.Context
	in           model.ClusterInput
	phase        string
	desired      int
	hosts        []hostEntry // ordered: workflow code must not depend on map order
	replacements int
	everReady    bool
	reserveSeq   int
}

func (c *clusterRun) find(id string) *hostEntry {
	for i := range c.hosts {
		if c.hosts[i].ID == id {
			return &c.hosts[i]
		}
	}
	return nil
}

func (c *clusterRun) count(states ...string) int {
	n := 0
	for _, h := range c.hosts {
		for _, s := range states {
			if h.State == s {
				n++
			}
		}
	}
	return n
}

func (c *clusterRun) active() int { return c.count(hostProvisioning, hostReady, hostDecommissioned) }

func (c *clusterRun) status() model.ClusterStatus {
	st := model.ClusterStatus{
		ID: c.in.ClusterID, Name: c.in.Name, Phase: c.phase, Spec: c.in.Spec,
		DesiredHosts: c.desired, ReadyHosts: c.count(hostReady), FailedHosts: c.count(hostFailed),
	}
	for _, h := range c.hosts {
		st.Hosts = append(st.Hosts, model.HostSummary{ID: h.ID, State: h.State})
	}
	return st
}

func (c *clusterRun) recompute() {
	if c.phase == model.PhaseTerminating || c.phase == model.PhaseTerminated {
		return
	}
	ready := c.count(hostReady)
	switch {
	case ready >= c.desired:
		c.phase, c.everReady = model.PhaseReady, true
	case c.count(hostFailed) > 0 && c.replacements >= maxReplacements:
		c.phase = model.PhaseDegraded
	case c.everReady:
		c.phase = model.PhaseScaling
	default:
		c.phase = model.PhaseProvisioning
	}
}

// addHosts reserves n hosts and starts a child lifecycle workflow for each.
// The reservation key makes the activity idempotent across retries.
func (c *clusterRun) addHosts(n int) error {
	c.reserveSeq++
	var ids []string
	err := workflow.ExecuteActivity(activityCtx(c.ctx), a.ReserveHosts, activities.ReserveInput{
		ClusterID: c.in.ClusterID, Key: fmt.Sprintf("batch-%d", c.reserveSeq), Count: n,
	}).Get(c.ctx, &ids)
	if err != nil {
		return err
	}
	parent := workflow.GetInfo(c.ctx).WorkflowExecution.ID
	for _, id := range ids {
		c.hosts = append(c.hosts, hostEntry{ID: id, State: hostProvisioning})
		child := workflow.WithChildOptions(c.ctx, workflow.ChildWorkflowOptions{WorkflowID: id})
		fut := workflow.ExecuteChildWorkflow(child, HostLifecycleWorkflow, model.HostLifecycleInput{
			ParentWorkflowID: parent,
			Spec: model.HostSpec{
				HostID: id, ClusterID: c.in.ClusterID, GPUModel: c.in.Spec.GPUModel,
				GPUCount: c.in.Spec.GPUsPerHost, Rack: c.in.Spec.Topology.Rack,
				DriverVersion: c.in.Spec.DriverVersion, CUDAVersion: c.in.Spec.CUDAVersion,
			},
		})
		// Wait only for the child to *start*; it keeps running for the host's life.
		if err := fut.GetChildWorkflowExecution().Get(c.ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *clusterRun) decommission(h *hostEntry) {
	h.State = hostDecommissioned
	err := workflow.SignalExternalWorkflow(c.ctx, h.ID, "", model.SignalDecommission,
		model.HostEvent{HostID: h.ID}).Get(c.ctx, nil)
	if err != nil { // child already finished
		workflow.GetLogger(c.ctx).Warn("decommission signal failed", "host", h.ID, "error", err)
		h.State = hostGone
	}
}

func (c *clusterRun) run() (model.ClusterStatus, error) {
	if err := c.addHosts(c.desired); err != nil {
		c.phase = model.PhaseDegraded
		return c.status(), err
	}

	chans := map[string]workflow.ReceiveChannel{
		model.SignalHostReady:  workflow.GetSignalChannel(c.ctx, model.SignalHostReady),
		model.SignalHostFailed: workflow.GetSignalChannel(c.ctx, model.SignalHostFailed),
		model.SignalHostGone:   workflow.GetSignalChannel(c.ctx, model.SignalHostGone),
		model.SignalScale:      workflow.GetSignalChannel(c.ctx, model.SignalScale),
		model.SignalTeardown:   workflow.GetSignalChannel(c.ctx, model.SignalTeardown),
	}
	order := []string{model.SignalHostReady, model.SignalHostFailed, model.SignalHostGone, model.SignalScale, model.SignalTeardown}

	for {
		if c.phase == model.PhaseTerminating && c.active() == 0 {
			c.phase = model.PhaseTerminated
			return c.status(), nil
		}

		// Callbacks only record events; all blocking work happens afterwards.
		var pending []pendingEvent
		sel := workflow.NewSelector(c.ctx)
		for _, name := range order {
			name := name
			sel.AddReceive(chans[name], func(ch workflow.ReceiveChannel, _ bool) {
				switch name {
				case model.SignalScale:
					var s model.ScaleSignal
					ch.Receive(c.ctx, &s)
					pending = append(pending, pendingEvent{kind: name, delta: s.DeltaHosts})
				case model.SignalTeardown:
					ch.Receive(c.ctx, nil)
					pending = append(pending, pendingEvent{kind: name})
				default:
					var ev model.HostEvent
					ch.Receive(c.ctx, &ev)
					pending = append(pending, pendingEvent{kind: name, hostID: ev.HostID})
				}
			})
		}
		sel.Select(c.ctx)

		for _, ev := range pending {
			if err := c.handle(ev); err != nil {
				return c.status(), err
			}
		}
		c.recompute()
	}
}

func (c *clusterRun) handle(ev pendingEvent) error {
	switch ev.kind {
	case model.SignalHostReady:
		if h := c.find(ev.hostID); h != nil && h.State == hostProvisioning {
			h.State = hostReady
		}
	case model.SignalHostGone:
		if h := c.find(ev.hostID); h != nil {
			h.State = hostGone
		}
	case model.SignalHostFailed:
		h := c.find(ev.hostID)
		if h == nil {
			return nil
		}
		h.State = hostFailed
		if c.phase != model.PhaseTerminating && c.replacements < maxReplacements {
			c.replacements++
			return c.addHosts(1) // self-healing at cluster level: replace lost capacity
		}
	case model.SignalScale:
		if c.phase == model.PhaseTerminating {
			return nil
		}
		target := c.desired + ev.delta
		if target < 1 {
			target = 1
		}
		if ev.delta > 0 {
			c.desired = target
			return c.addHosts(ev.delta)
		}
		c.desired = target
		for i := len(c.hosts) - 1; i >= 0 && c.count(hostProvisioning, hostReady) > c.desired; i-- {
			if c.hosts[i].State == hostReady || c.hosts[i].State == hostProvisioning {
				c.decommission(&c.hosts[i])
			}
		}
	case model.SignalTeardown:
		c.phase = model.PhaseTerminating
		for i := range c.hosts {
			if c.hosts[i].State == hostReady || c.hosts[i].State == hostProvisioning {
				c.decommission(&c.hosts[i])
			}
		}
	}
	return nil
}
