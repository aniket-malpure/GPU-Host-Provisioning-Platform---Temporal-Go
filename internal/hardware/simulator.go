package hardware

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type fault struct {
	remaining int
	permanent bool
}

type simHost struct {
	model     string
	gpus      int
	serial    string
	poweredOn bool
	osImage   string
	driver    string
	cuda      string
	healthy   bool
	broken    bool // hardware fault that Repair cannot fix
	cordoned  bool
	drained   bool
}

// Simulator is an in-memory Provider with fault injection.
type Simulator struct {
	mu           sync.Mutex
	hosts        map[string]*simHost
	reservations map[string][]string
	tickets      map[string]string
	faults       map[string]*fault
	calls        map[string]int
	nextHost     int
	nextTicket   int
}

// NewSimulator returns an empty simulator.
func NewSimulator() *Simulator {
	return &Simulator{
		hosts:        map[string]*simHost{},
		reservations: map[string][]string{},
		tickets:      map[string]string{},
		faults:       map[string]*fault{},
		calls:        map[string]int{},
	}
}

var _ Provider = (*Simulator)(nil)

// FailNext makes the next n invocations of op (any host) fail. Permanent
// failures return *PermanentError; others are plain, retryable errors.
func (s *Simulator) FailNext(op string, n int, permanent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[op] = &fault{remaining: n, permanent: permanent}
}

// SetHealthy flips the simulated health of a host (used to emulate a GPU
// falling off the bus, an ECC storm, etc.).
func (s *Simulator) SetHealthy(hostID string, healthy bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[hostID]
	if !ok {
		return fmt.Errorf("unknown host %q", hostID)
	}
	h.healthy = healthy
	return nil
}

// SetBroken marks a host as having a permanent hardware fault: it is
// unhealthy and Repair will not fix it.
func (s *Simulator) SetBroken(hostID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[hostID]
	if !ok {
		return fmt.Errorf("unknown host %q", hostID)
	}
	h.broken, h.healthy = true, false
	return nil
}

// SetDriver overrides the observed driver version (to emulate drift).
func (s *Simulator) SetDriver(hostID, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[hostID]
	if !ok {
		return fmt.Errorf("unknown host %q", hostID)
	}
	h.driver = version
	return nil
}

// Calls returns how many times op was invoked (including failed attempts).
func (s *Simulator) Calls(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

// RMATickets returns all opened RMA tickets, sorted.
func (s *Simulator) RMATickets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.tickets))
	for _, t := range s.tickets {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// begin records the call and applies any injected fault. Caller holds s.mu.
func (s *Simulator) begin(op string) error {
	s.calls[op]++
	if f, ok := s.faults[op]; ok && f.remaining > 0 {
		f.remaining--
		if f.permanent {
			return &PermanentError{Op: op, Reason: "injected permanent fault"}
		}
		return fmt.Errorf("%s: injected transient fault", op)
	}
	return nil
}

func (s *Simulator) with(op, hostID string, fn func(h *simHost) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(op); err != nil {
		return err
	}
	h, ok := s.hosts[hostID]
	if !ok {
		return &PermanentError{Op: op, Reason: "unknown host " + hostID}
	}
	return fn(h)
}

func (s *Simulator) Reserve(_ context.Context, clusterID, key string, n int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin("Reserve"); err != nil {
		return nil, err
	}
	rk := clusterID + "/" + key
	if ids, ok := s.reservations[rk]; ok { // idempotent on retry
		return append([]string(nil), ids...), nil
	}
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s.nextHost++
		id := fmt.Sprintf("gpu-node-%04d", s.nextHost)
		s.hosts[id] = &simHost{model: "H100", gpus: 8, serial: "SN" + id, healthy: true}
		ids = append(ids, id)
	}
	s.reservations[rk] = ids
	return append([]string(nil), ids...), nil
}

func (s *Simulator) Discover(_ context.Context, hostID string) (inv Inventory, err error) {
	err = s.with("Discover", hostID, func(h *simHost) error {
		inv = Inventory{Model: h.model, GPUCount: h.gpus, Serial: h.serial}
		return nil
	})
	return inv, err
}

func (s *Simulator) PowerCycle(_ context.Context, hostID string) error {
	return s.with("PowerCycle", hostID, func(h *simHost) error { h.poweredOn = true; return nil })
}

func (s *Simulator) InstallOS(_ context.Context, hostID, image string) error {
	return s.with("InstallOS", hostID, func(h *simHost) error {
		if !h.poweredOn {
			return errors.New("InstallOS: host not powered on")
		}
		h.osImage = image // idempotent: same image is a no-op
		return nil
	})
}

func (s *Simulator) InstallGPUStack(_ context.Context, hostID, driver, cuda string) error {
	return s.with("InstallGPUStack", hostID, func(h *simHost) error {
		if h.osImage == "" {
			return errors.New("InstallGPUStack: OS not installed")
		}
		h.driver, h.cuda = driver, cuda
		return nil
	})
}

func (s *Simulator) Inspect(_ context.Context, hostID string) (o Observed, err error) {
	err = s.with("Inspect", hostID, func(h *simHost) error {
		o = Observed{DriverVersion: h.driver, CUDAVersion: h.cuda, GPUCount: h.gpus,
			OSImage: h.osImage, PoweredOn: h.poweredOn, Cordoned: h.cordoned}
		return nil
	})
	return o, err
}

func (s *Simulator) RunHealthCheck(_ context.Context, hostID string) (rep HealthReport, err error) {
	err = s.with("RunHealthCheck", hostID, func(h *simHost) error {
		stack := h.poweredOn && h.driver != ""
		rep.Checks = []Check{
			{Name: "gpu-stack", Pass: stack, Detail: "driver/CUDA present and host powered"},
			{Name: "ecc-nvlink", Pass: h.healthy, Detail: "ECC and NVLink error counters"},
		}
		rep.Healthy = stack && h.healthy
		return nil
	})
	return rep, err
}

func (s *Simulator) Cordon(_ context.Context, hostID string) error {
	return s.with("Cordon", hostID, func(h *simHost) error { h.cordoned = true; return nil })
}

func (s *Simulator) Drain(_ context.Context, hostID string) error {
	return s.with("Drain", hostID, func(h *simHost) error { h.cordoned, h.drained = true, true; return nil })
}

func (s *Simulator) Uncordon(_ context.Context, hostID string) error {
	return s.with("Uncordon", hostID, func(h *simHost) error { h.cordoned, h.drained = false, false; return nil })
}

func (s *Simulator) Repair(_ context.Context, hostID string) error {
	return s.with("Repair", hostID, func(h *simHost) error {
		h.poweredOn = true
		if !h.broken {
			h.healthy = true
		}
		return nil
	})
}

func (s *Simulator) PowerOff(_ context.Context, hostID string) error {
	return s.with("PowerOff", hostID, func(h *simHost) error { h.poweredOn = false; return nil })
}

func (s *Simulator) Wipe(_ context.Context, hostID string) error {
	return s.with("Wipe", hostID, func(h *simHost) error { h.osImage, h.driver, h.cuda = "", "", ""; return nil })
}

func (s *Simulator) OpenRMA(_ context.Context, hostID, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin("OpenRMA"); err != nil {
		return "", err
	}
	if t, ok := s.tickets[hostID]; ok { // idempotent
		return t, nil
	}
	s.nextTicket++
	t := fmt.Sprintf("RMA-%04d", s.nextTicket)
	s.tickets[hostID] = t
	return t, nil
}
