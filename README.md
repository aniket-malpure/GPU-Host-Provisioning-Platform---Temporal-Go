# GPU Host Provisioning Platform (Temporal & Go)

A control plane that takes a GPU host from discovery to production capacity and keeps it healthy, as software rather than runbooks.

- **Provisioning state machine.** Each physical host is a durable Temporal workflow with explicit, versioned states: `DISCOVERED → PROVISIONING → DRIVER_INSTALL → VALIDATING → READY`, plus `DEGRADED → DRAINING → REPAIRING` for self-healing and `RMA / DECOMMISSIONED` at end of life.
- **Self-service, declarative API.** One call with a manifest creates a cluster. No ticket, no human in the loop. `gpuctl` is the CLI over the same API.
- **Self-healing.** A health event on the stream wakes the host's workflow, which cordons and drains the node, repairs it, reconverges the GPU stack, re-validates and returns it to the pool. After a fixed repair budget the host is retired to RMA and the cluster workflow requests a replacement.
- **Reliability.** Idempotent activities, bounded retries with exponential backoff, no retry on permanent hardware faults, compensation on failed bring-up, periodic drift detection with a durable timer, and `ContinueAsNew` to bound history.
- **Event-driven.** State changes and health signals flow over Kafka (or an in-process bus for tests). Nothing polls or runs from cron.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the state diagram and design notes.

## Layout

```
cmd/worker         Temporal worker (+ optional remediator and simulator fault injection)
cmd/api            REST API server
cmd/gpuctl         self-service CLI
internal/domain    pure host state machine (no I/O, deterministic)
internal/workflows HostLifecycleWorkflow, ClusterWorkflow (Temporal)
internal/activities side-effecting steps, idempotent, SDK-free
internal/hardware  Provider interface + simulator with fault injection
internal/drift     desired vs observed comparison
internal/events    event model, in-memory bus, Kafka bus (-tags kafka)
internal/remediation health event -> workflow signal
internal/orchestrator Temporal client adapter
internal/api       REST handlers
internal/model     manifest and shared types
api/proto          optional gRPC contract (not implemented, see note)
deploy/            docker-compose (Temporal, Redpanda, worker, API)
```

## Quick start

Requires Go 1.24+ and network access for the first dependency resolution.

```bash
make tidy        # resolve and lock go.temporal.io/sdk, go.temporal.io/api, kafka-go
make test        # unit + Temporal workflow tests (race detector)
make build
```

Run the full stack (Temporal UI at http://localhost:8233):

```bash
make up
export GPUCTL_SERVER=http://localhost:8080
./bin/gpuctl apply -f examples/cluster.json
./bin/gpuctl wait cluster llama-prod
./bin/gpuctl get cluster llama-prod
```

Break something and watch it heal (or run `make demo`):

```bash
curl -X POST 'localhost:9090/faults/degrade?host=gpu-node-0001'   # repairable fault
curl -X POST 'localhost:9090/faults/break?host=gpu-node-0001'      # permanent fault -> RMA -> replacement
./bin/gpuctl get host gpu-node-0001
```

## Manifest

```json
{
  "apiVersion": "gpu.platform/v1",
  "kind": "InferenceCluster",
  "metadata": { "name": "llama-prod" },
  "spec": {
    "gpuModel": "H100",
    "gpus": 16,
    "gpusPerHost": 8,
    "topology": { "interconnect": "infiniband", "rack": "r12" },
    "driverVersion": "550.90.07",
    "cudaVersion": "12.4"
  }
}
```

Re-applying the same manifest is idempotent. Changing only `spec.gpus` scales the cluster; every other field is immutable and rejected with `422`.

## REST API

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/clusters` | create a cluster (202; 200 if it already exists) |
| PUT | `/v1/clusters/{name}` | scale by changing `spec.gpus` |
| GET | `/v1/clusters/{name}` | status, phase and per-host state |
| DELETE | `/v1/clusters/{name}` | tear down |
| GET | `/v1/hosts/{id}` | host state and transition history |
| DELETE | `/v1/hosts/{id}` | decommission one host |
| POST | `/v1/hosts/{id}/health-events` | publish a degraded-health event |

## What is simulated

Hardware access (BMC/Redfish, PXE, driver install, drain) sits behind `hardware.Provider`. The bundled `Simulator` implements it in memory with fault injection so the whole lifecycle runs and is testable without GPUs. A production provider would implement the same interface against Redfish/IPMI, an iPXE service, node agents and the Kubernetes API. The workflows, state machine, API and event plumbing do not change.

## Notes and limits

- **Dependencies are not pinned.** `go.mod` ships without `require` entries; run `make tidy` once to resolve and lock the Temporal SDK, API module and kafka-go.
- **Kafka is a build tag.** `go build -tags kafka` (used by the Dockerfile) selects the Kafka bus when `KAFKA_BROKERS` is set. The default build uses an in-process bus, which does not cross process boundaries, so the API-to-remediator path needs the Kafka build.
- **gRPC is not implemented.** `api/proto/gpu/v1/provisioner.proto` is a contract sketch. The shipped transport is REST; the `api.Orchestrator` interface is transport-agnostic if you add gRPC.
- **Cluster workflow history is unbounded.** Host workflows use `ContinueAsNew`; the cluster workflow does not, which is fine for modest cluster sizes but should be added for very long-lived or very large clusters.
- **Repair budget is lifetime, not rolling.** A host that needs more than 3 repairs over its life is retired to RMA by design, so flapping hardware is eventually pulled.
