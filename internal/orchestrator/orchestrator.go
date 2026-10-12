// Package orchestrator adapts the Temporal client to the api.Orchestrator and
// remediation.Signaler interfaces.
package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/workflows"
)

// Client talks to a Temporal cluster.
type Client struct{ C client.Client }

// Dial connects to Temporal.
func Dial(hostPort, namespace string) (*Client, error) {
	c, err := client.Dial(client.Options{HostPort: hostPort, Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	return &Client{C: c}, nil
}

// Close releases the connection.
func (o *Client) Close() { o.C.Close() }

func clusterID(name string) string { return "cluster-" + name }

func mapErr(err error) error {
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return model.ErrNotFound
	}
	return err
}

// StartCluster starts the cluster workflow. The workflow ID is derived from
// the cluster name, so applying the same manifest twice is a no-op.
func (o *Client) StartCluster(ctx context.Context, m model.Manifest) (bool, error) {
	_, err := o.C.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        clusterID(m.Metadata.Name),
		TaskQueue: workflows.TaskQueue,
	}, workflows.ClusterWorkflow, model.ClusterInput{
		ClusterID: clusterID(m.Metadata.Name), Name: m.Metadata.Name, Spec: m.Spec,
	})
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		return false, nil
	}
	return err == nil, err
}

func (o *Client) ClusterStatus(ctx context.Context, name string) (model.ClusterStatus, error) {
	var st model.ClusterStatus
	v, err := o.C.QueryWorkflow(ctx, clusterID(name), "", model.QueryCluster)
	if err != nil {
		return st, mapErr(err)
	}
	return st, v.Get(&st)
}

func (o *Client) ScaleCluster(ctx context.Context, name string, deltaHosts int) error {
	return mapErr(o.C.SignalWorkflow(ctx, clusterID(name), "", model.SignalScale, model.ScaleSignal{DeltaHosts: deltaHosts}))
}

func (o *Client) DeleteCluster(ctx context.Context, name string) error {
	return mapErr(o.C.SignalWorkflow(ctx, clusterID(name), "", model.SignalTeardown, nil))
}

func (o *Client) HostStatus(ctx context.Context, hostID string) (domain.Host, error) {
	var h domain.Host
	v, err := o.C.QueryWorkflow(ctx, hostID, "", model.QueryHost)
	if err != nil {
		return h, mapErr(err)
	}
	return h, v.Get(&h)
}

func (o *Client) DecommissionHost(ctx context.Context, hostID string) error {
	return mapErr(o.C.SignalWorkflow(ctx, hostID, "", model.SignalDecommission, model.HostEvent{HostID: hostID}))
}

// SignalDegraded implements remediation.Signaler.
func (o *Client) SignalDegraded(ctx context.Context, hostID, reason string) error {
	return mapErr(o.C.SignalWorkflow(ctx, hostID, "", model.SignalDegraded, model.DegradedSignal{Reason: reason}))
}
