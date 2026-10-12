package drift

import (
	"strings"
	"testing"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/hardware"
)

func TestDetect(t *testing.T) {
	want := Desired{DriverVersion: "550.90", CUDAVersion: "12.4", GPUCount: 8}
	good := hardware.Observed{DriverVersion: "550.90", CUDAVersion: "12.4", GPUCount: 8, PoweredOn: true}

	cases := []struct {
		name string
		obs  hardware.Observed
		want []string
	}{
		{"no drift", good, nil},
		{"driver", func() hardware.Observed { o := good; o.DriverVersion = "535.1"; return o }(), []string{"driverVersion"}},
		{"cuda+gpus", func() hardware.Observed { o := good; o.CUDAVersion = "12.1"; o.GPUCount = 7; return o }(), []string{"cudaVersion", "gpuCount"}},
		{"powered off", func() hardware.Observed { o := good; o.PoweredOn = false; return o }(), []string{"poweredOn"}},
	}
	for _, c := range cases {
		got := Detect(want, c.obs)
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v, want fields %v", c.name, got, c.want)
			continue
		}
		for i, f := range c.want {
			if got[i].Field != f {
				t.Errorf("%s: field %d = %s, want %s", c.name, i, got[i].Field, f)
			}
		}
	}
}

func TestEmptyDesiredIsIgnoredAndSummary(t *testing.T) {
	if ds := Detect(Desired{}, hardware.Observed{PoweredOn: true, DriverVersion: "x"}); len(ds) != 0 {
		t.Fatalf("unexpected drift: %v", ds)
	}
	s := Summary([]Drift{{Field: "driverVersion", Want: "a", Got: "b"}})
	if !strings.Contains(s, "driverVersion want=a got=b") {
		t.Fatalf("summary = %q", s)
	}
}
