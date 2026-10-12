// Package drift compares desired host configuration against observed state.
package drift

import (
	"fmt"
	"strings"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
)

// Desired is the declared configuration for a host.
type Desired struct {
	DriverVersion string `json:"driverVersion"`
	CUDAVersion   string `json:"cudaVersion"`
	GPUCount      int    `json:"gpuCount"`
}

// Drift is one divergence between desired and observed.
type Drift struct {
	Field string `json:"field"`
	Want  string `json:"want"`
	Got   string `json:"got"`
}

// Detect returns every field where observed differs from desired. Empty
// desired fields are ignored (unspecified means "don't care").
func Detect(d Desired, o hardware.Observed) []Drift {
	var out []Drift
	add := func(field, want, got string) {
		if want != "" && want != got {
			out = append(out, Drift{Field: field, Want: want, Got: got})
		}
	}
	add("driverVersion", d.DriverVersion, o.DriverVersion)
	add("cudaVersion", d.CUDAVersion, o.CUDAVersion)
	if d.GPUCount > 0 && d.GPUCount != o.GPUCount {
		out = append(out, Drift{Field: "gpuCount", Want: fmt.Sprint(d.GPUCount), Got: fmt.Sprint(o.GPUCount)})
	}
	if !o.PoweredOn {
		out = append(out, Drift{Field: "poweredOn", Want: "true", Got: "false"})
	}
	return out
}

// Summary renders drifts for logs and state-transition reasons.
func Summary(ds []Drift) string {
	parts := make([]string, 0, len(ds))
	for _, d := range ds {
		parts = append(parts, fmt.Sprintf("%s want=%s got=%s", d.Field, d.Want, d.Got))
	}
	return strings.Join(parts, "; ")
}
