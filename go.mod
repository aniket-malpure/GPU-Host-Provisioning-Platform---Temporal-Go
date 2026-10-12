module github.com/aniketmalpure/gpu-host-provisioner

go 1.24

// Third-party dependencies (go.temporal.io/sdk, go.temporal.io/api and
// github.com/segmentio/kafka-go) are intentionally not pinned here. Run
// `make tidy` (go mod tidy) once to resolve and lock them in go.mod/go.sum.
