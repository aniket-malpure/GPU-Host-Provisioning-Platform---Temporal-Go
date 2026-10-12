package model

import "testing"

func valid() Manifest {
	return Manifest{
		Metadata: Metadata{Name: "llama-prod"},
		Spec:     ClusterSpec{GPUs: 32, Topology: Topology{Interconnect: "infiniband"}},
	}.Normalize()
}

func TestNormalizeDefaults(t *testing.T) {
	m := valid()
	if m.APIVersion != APIVersion || m.Kind != KindInferenceCluster ||
		m.Spec.GPUsPerHost != 8 || m.Spec.DriverVersion == "" || m.Spec.CUDAVersion == "" || m.Spec.GPUModel != "H100" {
		t.Fatalf("defaults not applied: %+v", m)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.Spec.Hosts() != 4 {
		t.Fatalf("hosts = %d, want 4", m.Spec.Hosts())
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Manifest){
		"bad name":      func(m *Manifest) { m.Metadata.Name = "Bad_Name" },
		"empty name":    func(m *Manifest) { m.Metadata.Name = "" },
		"zero gpus":     func(m *Manifest) { m.Spec.GPUs = 0 },
		"too many":      func(m *Manifest) { m.Spec.GPUs = 8192 },
		"not multiple":  func(m *Manifest) { m.Spec.GPUs = 12 },
		"bad per host":  func(m *Manifest) { m.Spec.GPUsPerHost = 3 },
		"bad fabric":    func(m *Manifest) { m.Spec.Topology.Interconnect = "carrier-pigeon" },
		"bad kind":      func(m *Manifest) { m.Kind = "Pod" },
		"bad apiVerion": func(m *Manifest) { m.APIVersion = "v9" },
	}
	for name, mutate := range cases {
		m := valid()
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestHostsRoundsUp(t *testing.T) {
	if got := (ClusterSpec{GPUs: 12, GPUsPerHost: 8}).Hosts(); got != 2 {
		t.Fatalf("got %d", got)
	}
	if got := (ClusterSpec{GPUs: 8}).Hosts(); got != 0 {
		t.Fatalf("zero gpusPerHost should yield 0, got %d", got)
	}
}

func TestImmutableDiff(t *testing.T) {
	a := valid().Spec
	b := a
	b.GPUs = 64 // allowed
	if d := ImmutableDiff(a, b); len(d) != 0 {
		t.Fatalf("gpus change should be allowed, got %v", d)
	}
	b.DriverVersion = "999"
	b.Topology.Rack = "r9"
	if d := ImmutableDiff(a, b); len(d) != 2 {
		t.Fatalf("want 2 immutable diffs, got %v", d)
	}
}
