package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/drift"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

// HostLifecycleWorkflow owns one physical host from discovery to
// decommission/RMA. The workflow *is* the state machine: state lives in
// workflow history, so it survives worker crashes and resumes mid-step.
//
//	DISCOVERED -> PROVISIONING -> DRIVER_INSTALL -> VALIDATING -> READY
//	READY -> DEGRADED -> DRAINING -> REPAIRING -> VALIDATING -> READY  (self-healing)
//	any failure -> FAILED/RMA -> DECOMMISSIONED
func HostLifecycleWorkflow(ctx workflow.Context, in model.HostLifecycleInput) (model.HostLifecycleResult, error) {
	// Versioning hook: bump the max version when changing workflow logic in a
	// way that is incompatible with in-flight executions.
	_ = workflow.GetVersion(ctx, "host-lifecycle", workflow.DefaultVersion, 1)

	r := &hostRun{ctx: ctx, in: in}
	if err := workflow.SetQueryHandler(ctx, model.QueryHost, func() (domain.Host, error) { return r.host, nil }); err != nil {
		return model.HostLifecycleResult{}, err
	}

	if in.Resume {
		r.host = in.Host
	} else {
		s := in.Spec
		r.host = domain.NewHost(s.HostID, s.ClusterID, s.GPUModel, s.GPUCount, workflow.Now(ctx))
		r.host.Rack, r.host.DesiredDriver, r.host.DesiredCUDA = s.Rack, s.DriverVersion, s.CUDAVersion
		if err := r.bringUp(); err != nil {
			return r.failBringUp(err)
		}
		r.notifyParent(model.SignalHostReady, "bring-up complete")
	}
	return r.serve()
}

type hostRun struct {
	ctx  workflow.Context
	in   model.HostLifecycleInput
	host domain.Host
}

func (r *hostRun) ref() activities.HostRef { return activities.HostRef{HostID: r.in.Spec.HostID} }

func (r *hostRun) exec(fn, arg, result interface{}) error {
	return workflow.ExecuteActivity(activityCtx(r.ctx), fn, arg).Get(r.ctx, result)
}

// move performs a validated state transition and publishes the change event.
// An illegal transition is a programming error and fails the workflow.
func (r *hostRun) move(to domain.State, reason string) error {
	if err := r.host.Transition(to, reason, workflow.Now(r.ctx)); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "InvalidTransition", err)
	}
	last := r.host.History[len(r.host.History)-1]
	pubCtx := workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	err := workflow.ExecuteActivity(pubCtx, a.PublishStateChange, activities.StateChange{
		HostID: r.host.ID, ClusterID: r.host.ClusterID,
		From: string(last.From), To: string(last.To), Reason: reason, Version: last.Version, At: last.At,
	}).Get(r.ctx, nil)
	if err != nil { // observability must never block provisioning
		workflow.GetLogger(r.ctx).Warn("state-change publish failed", "host", r.host.ID, "error", err)
	}
	return nil
}

// bringUp: discover -> PXE/OS -> GPU driver+CUDA -> validate -> READY.
func (r *hostRun) bringUp() error {
	var inv hardware.Inventory
	if err := r.exec(a.DiscoverHardware, r.ref(), &inv); err != nil {
		return err
	}
	if want := r.in.Spec.GPUCount; want > 0 && inv.GPUCount != want {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("inventory mismatch: want %d GPUs, found %d", want, inv.GPUCount),
			hardware.PermanentErrorType, nil)
	}

	if err := r.move(domain.StateProvisioning, "power cycle and network boot"); err != nil {
		return err
	}
	if err := r.exec(a.ProvisionOS, r.ref(), nil); err != nil {
		return err
	}

	if err := r.move(domain.StateDriverInstall, "installing GPU driver and CUDA"); err != nil {
		return err
	}
	if err := r.installStack(); err != nil {
		return err
	}

	if err := r.move(domain.StateValidating, "running health validation"); err != nil {
		return err
	}
	ok, err := r.validate()
	if err != nil {
		return err
	}
	if !ok {
		return temporal.NewNonRetryableApplicationError("health validation failed", hardware.PermanentErrorType, nil)
	}
	return r.move(domain.StateReady, "validated")
}

func (r *hostRun) installStack() error {
	return r.exec(a.InstallGPUStack, activities.StackInput{
		HostID: r.in.Spec.HostID, Driver: r.in.Spec.DriverVersion, CUDA: r.in.Spec.CUDAVersion,
	}, nil)
}

func (r *hostRun) validate() (bool, error) {
	var rep hardware.HealthReport
	if err := r.exec(a.ValidateHealth, r.ref(), &rep); err != nil {
		return false, err
	}
	return rep.Healthy, nil
}

// detach switches to a disconnected context so compensation still runs if the
// workflow was cancelled; the returned func restores the original.
func (r *hostRun) detach() func() {
	saved := r.ctx
	r.ctx, _ = workflow.NewDisconnectedContext(r.ctx)
	return func() { r.ctx = saved }
}

// failBringUp compensates a failed bring-up: release the hardware and either
// send it back to the vendor (permanent fault) or just free it (exhausted
// retries on a transient fault), then tell the parent cluster.
func (r *hostRun) failBringUp(cause error) (model.HostLifecycleResult, error) {
	defer r.detach()()
	reason := "bring-up failed: " + cause.Error()
	if err := r.move(domain.StateFailed, reason); err != nil {
		return model.HostLifecycleResult{}, err
	}
	if err := r.retire(reason, isPermanent(cause)); err != nil {
		return model.HostLifecycleResult{}, err
	}
	r.notifyParent(model.SignalHostFailed, reason)
	return r.result(reason), nil
}

// retire releases the host: drain, power off, wipe, then RMA or decommission.
func (r *hostRun) retire(reason string, rma bool) error {
	defer r.detach()()
	if err := r.exec(a.DecommissionHost, r.ref(), nil); err != nil {
		workflow.GetLogger(r.ctx).Warn("teardown incomplete", "host", r.host.ID, "error", err)
	}
	if rma {
		var ticket string
		if err := r.exec(a.OpenRMA, activities.RMAInput{HostID: r.host.ID, Reason: reason}, &ticket); err != nil {
			return err
		}
		if err := r.move(domain.StateRMA, fmt.Sprintf("%s (ticket %s)", reason, ticket)); err != nil {
			return err
		}
	} else if r.host.State != domain.StateDecommissioning {
		if err := r.move(domain.StateDecommissioning, reason); err != nil {
			return err
		}
	}
	return r.move(domain.StateDecommissioned, "hardware released")
}

// serve is the steady-state loop for a READY host. It blocks on signals and a
// durable drift timer: event-driven, no polling or cron.
func (r *hostRun) serve() (model.HostLifecycleResult, error) {
	degraded := workflow.GetSignalChannel(r.ctx, model.SignalDegraded)
	decommission := workflow.GetSignalChannel(r.ctx, model.SignalDecommission)

	for loops := 0; ; loops++ {
		if loops >= maxLoopsBeforeContinueAsNew {
			next := r.in
			next.Resume, next.Host = true, r.host
			return model.HostLifecycleResult{}, workflow.NewContinueAsNewError(r.ctx, HostLifecycleWorkflow, next)
		}

		timerCtx, cancelTimer := workflow.WithCancel(r.ctx)
		timer := workflow.NewTimer(timerCtx, driftInterval)

		action, reason := "", ""
		sel := workflow.NewSelector(r.ctx)
		sel.AddReceive(degraded, func(c workflow.ReceiveChannel, _ bool) {
			var s model.DegradedSignal
			c.Receive(r.ctx, &s)
			action, reason = "degraded", s.Reason
		})
		sel.AddReceive(decommission, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(r.ctx, nil)
			action = "decommission"
		})
		sel.AddFuture(timer, func(f workflow.Future) {
			if f.Get(r.ctx, nil) == nil {
				action = "drift"
			}
		})
		sel.Select(r.ctx)
		cancelTimer()

		switch action {
		case "decommission":
			if err := r.retire("decommission requested", false); err != nil {
				return model.HostLifecycleResult{}, err
			}
			r.notifyParent(model.SignalHostGone, "decommissioned")
			return r.result("decommission requested"), nil

		case "degraded":
			retired, err := r.remediate(reason)
			if err != nil {
				return model.HostLifecycleResult{}, err
			}
			if retired {
				r.notifyParent(model.SignalHostFailed, "retired after failed remediation")
				return r.result("retired after failed remediation"), nil
			}
			// Collapse a burst of duplicate health events into one remediation.
			var dup model.DegradedSignal
			for degraded.ReceiveAsync(&dup) {
			}

		case "drift":
			var drifts []drift.Drift
			err := r.exec(a.DetectDrift, activities.DriftInput{
				HostID: r.in.Spec.HostID,
				Desired: drift.Desired{
					DriverVersion: r.in.Spec.DriverVersion,
					CUDAVersion:   r.in.Spec.CUDAVersion,
					GPUCount:      r.in.Spec.GPUCount,
				},
			}, &drifts)
			if err != nil {
				workflow.GetLogger(r.ctx).Warn("drift check failed", "host", r.host.ID, "error", err)
				continue
			}
			if len(drifts) == 0 {
				continue
			}
			retired, err := r.remediate("drift: " + drift.Summary(drifts))
			if err != nil {
				return model.HostLifecycleResult{}, err
			}
			if retired {
				r.notifyParent(model.SignalHostFailed, "retired after drift remediation failed")
				return r.result("retired after drift remediation failed"), nil
			}
		}
	}
}

// remediate runs the self-healing loop: degrade, drain, repair and reconverge
// the GPU stack, re-validate, and return the host to the pool. After
// maxRepairAttempts it retires the host to RMA instead. It reports whether the
// host was retired.
func (r *hostRun) remediate(reason string) (retired bool, err error) {
	giveUp := func(why string) (bool, error) {
		if r.host.State == domain.StateValidating { // not a legal source for RMA retire
			if err := r.move(domain.StateDegraded, why); err != nil {
				return false, err
			}
		}
		return true, r.retire(why, true)
	}

	for {
		if r.host.State != domain.StateDegraded {
			if err := r.move(domain.StateDegraded, reason); err != nil {
				return false, err
			}
		}
		if r.in.RepairAttempts >= maxRepairAttempts {
			return giveUp("repair budget exhausted: " + reason)
		}
		r.in.RepairAttempts++

		if err := r.move(domain.StateDraining, "cordon and drain"); err != nil {
			return false, err
		}
		if err := r.exec(a.CordonAndDrain, r.ref(), nil); err != nil {
			return giveUp("drain failed: " + err.Error())
		}
		if err := r.move(domain.StateRepairing, fmt.Sprintf("repair attempt %d", r.in.RepairAttempts)); err != nil {
			return false, err
		}
		if err := r.exec(a.RepairHost, r.ref(), nil); err != nil {
			return giveUp("repair failed: " + err.Error())
		}
		if err := r.installStack(); err != nil { // reconcile driver/CUDA to desired
			return giveUp("stack reinstall failed: " + err.Error())
		}
		if err := r.move(domain.StateValidating, "re-validating after repair"); err != nil {
			return false, err
		}
		ok, err := r.validate()
		if err != nil {
			return giveUp("validation error: " + err.Error())
		}
		if ok {
			if err := r.exec(a.Uncordon, r.ref(), nil); err != nil {
				return false, err
			}
			return false, r.move(domain.StateReady, "recovered after repair")
		}
		reason = fmt.Sprintf("validation failed after repair attempt %d", r.in.RepairAttempts)
	}
}

// notifyParent signals the owning cluster workflow, if any. Failures are
// logged, not fatal: a missing parent must not strand the host.
func (r *hostRun) notifyParent(signal, reason string) {
	if r.in.ParentWorkflowID == "" {
		return
	}
	err := workflow.SignalExternalWorkflow(r.ctx, r.in.ParentWorkflowID, "", signal,
		model.HostEvent{HostID: r.host.ID, Reason: reason}).Get(r.ctx, nil)
	if err != nil {
		workflow.GetLogger(r.ctx).Warn("could not signal parent", "signal", signal, "error", err)
	}
}

func (r *hostRun) result(reason string) model.HostLifecycleResult {
	return model.HostLifecycleResult{HostID: r.host.ID, FinalState: r.host.State, Reason: reason, Host: r.host}
}
