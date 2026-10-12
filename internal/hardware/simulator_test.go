package hardware

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

var ctx = context.Background()

func newHost(t *testing.T) (*Simulator, string) {
	t.Helper()
	s := NewSimulator()
	ids, err := s.Reserve(ctx, "c1", "initial", 1)
	if err != nil || len(ids) != 1 {
		t.Fatalf("reserve: %v %v", ids, err)
	}
	return s, ids[0]
}

func TestPermanentErrorTypeNameMatchesTemporalConvention(t *testing.T) {
	// Temporal names non-Temporal errors by their Go type name; the retry
	// policy's NonRetryableErrorTypes depends on this staying in sync.
	got := reflect.TypeOf(&PermanentError{}).Elem().Name()
	if got != PermanentErrorType {
		t.Fatalf("type name %q != PermanentErrorType %q", got, PermanentErrorType)
	}
}

func TestReserveIsIdempotent(t *testing.T) {
	s := NewSimulator()
	a, _ := s.Reserve(ctx, "c1", "initial", 3)
	b, _ := s.Reserve(ctx, "c1", "initial", 3)
	c, _ := s.Reserve(ctx, "c1", "scale-1", 2)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("retry returned different hosts: %v vs %v", a, b)
	}
	for _, id := range c {
		for _, x := range a {
			if id == x {
				t.Fatalf("host %s reserved twice", id)
			}
		}
	}
}

func TestBringUpIsIdempotent(t *testing.T) {
	s, id := newHost(t)
	for i := 0; i < 2; i++ {
		if err := s.PowerCycle(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := s.InstallOS(ctx, id, "ubuntu-24.04"); err != nil {
			t.Fatal(err)
		}
		if err := s.InstallGPUStack(ctx, id, "550.90", "12.4"); err != nil {
			t.Fatal(err)
		}
	}
	rep, _ := s.RunHealthCheck(ctx, id)
	if !rep.Healthy {
		t.Fatalf("expected healthy: %+v", rep)
	}
}

func TestOrderingIsEnforced(t *testing.T) {
	s, id := newHost(t)
	if err := s.InstallOS(ctx, id, "img"); err == nil {
		t.Fatal("InstallOS before power should fail")
	}
	if err := s.InstallGPUStack(ctx, id, "550", "12.4"); err == nil {
		t.Fatal("InstallGPUStack before OS should fail")
	}
}

func TestFaultInjection(t *testing.T) {
	s, id := newHost(t)
	s.FailNext("PowerCycle", 2, false)
	for i := 0; i < 2; i++ {
		err := s.PowerCycle(ctx, id)
		if err == nil || IsPermanent(err) {
			t.Fatalf("attempt %d: want transient error, got %v", i, err)
		}
	}
	if err := s.PowerCycle(ctx, id); err != nil {
		t.Fatalf("third attempt should succeed: %v", err)
	}
	if s.Calls("PowerCycle") != 3 {
		t.Fatalf("calls = %d, want 3", s.Calls("PowerCycle"))
	}

	s.FailNext("Discover", 1, true)
	_, err := s.Discover(ctx, id)
	var p *PermanentError
	if !errors.As(err, &p) {
		t.Fatalf("want PermanentError, got %v", err)
	}
}

func TestUnknownHostIsPermanent(t *testing.T) {
	s := NewSimulator()
	if err := s.PowerCycle(ctx, "nope"); !IsPermanent(err) {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestRepairFixesSoftFaultsButNotBrokenHardware(t *testing.T) {
	s, id := newHost(t)
	_ = s.PowerCycle(ctx, id)
	_ = s.InstallOS(ctx, id, "img")
	_ = s.InstallGPUStack(ctx, id, "550", "12.4")

	_ = s.SetHealthy(id, false)
	_ = s.Repair(ctx, id)
	if rep, _ := s.RunHealthCheck(ctx, id); !rep.Healthy {
		t.Fatal("soft fault should be repaired")
	}

	_ = s.SetBroken(id)
	_ = s.Repair(ctx, id)
	if rep, _ := s.RunHealthCheck(ctx, id); rep.Healthy {
		t.Fatal("broken hardware must stay unhealthy")
	}
}

func TestRMAIsIdempotent(t *testing.T) {
	s, id := newHost(t)
	a, _ := s.OpenRMA(ctx, id, "dead gpu")
	b, _ := s.OpenRMA(ctx, id, "dead gpu")
	if a != b || len(s.RMATickets()) != 1 {
		t.Fatalf("tickets: %q %q %v", a, b, s.RMATickets())
	}
}
