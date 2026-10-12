// Package workflows holds the durable Temporal workflows. Everything in here
// must be deterministic: time comes from workflow.Now, waiting uses durable
// timers and signals, and all I/O happens in activities.
package workflows

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/activities"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
)

// TaskQueue is the queue the provisioning worker polls.
const TaskQueue = "gpu-provisioning"

const (
	// maxRepairAttempts is the self-healing budget before a host is retired
	// to RMA. It is never reset, so a flapping host is eventually pulled.
	maxRepairAttempts = 3
	// driftInterval is how often a READY host is reconciled against desired
	// state. It is a durable timer, not a cron job.
	driftInterval = 15 * time.Minute
	// maxLoopsBeforeContinueAsNew bounds workflow history size.
	maxLoopsBeforeContinueAsNew = 50
	// maxReplacements bounds automatic replacement of failed cluster hosts.
	maxReplacements = 4
)

// a is only used to reference activity methods by value.
var a *activities.Activities

// activityCtx applies the retry policy shared by all provisioning activities:
// exponential backoff for transient faults, no retry for permanent hardware
// failures (those go to the RMA path instead).
func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute, // OS installs are slow
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        2 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        2 * time.Minute,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{hardware.PermanentErrorType},
		},
	})
}

// isPermanent reports whether err came from a permanent hardware failure.
func isPermanent(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == hardware.PermanentErrorType
}
