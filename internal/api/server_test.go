package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

type fakeOrch struct {
	clusters  map[string]model.ClusterStatus
	scaled    map[string]int
	deleted   []string
	decom     []string
	startedOK int
}

func newFake() *fakeOrch {
	return &fakeOrch{clusters: map[string]model.ClusterStatus{}, scaled: map[string]int{}}
}

func (f *fakeOrch) StartCluster(_ context.Context, m model.Manifest) (bool, error) {
	if _, ok := f.clusters[m.Metadata.Name]; ok {
		return false, nil
	}
	f.startedOK++
	f.clusters[m.Metadata.Name] = model.ClusterStatus{
		Name: m.Metadata.Name, Phase: model.PhaseProvisioning, Spec: m.Spec, DesiredHosts: m.Spec.Hosts(),
	}
	return true, nil
}
func (f *fakeOrch) ClusterStatus(_ context.Context, n string) (model.ClusterStatus, error) {
	c, ok := f.clusters[n]
	if !ok {
		return c, model.ErrNotFound
	}
	return c, nil
}
func (f *fakeOrch) ScaleCluster(_ context.Context, n string, d int) error {
	f.scaled[n] = d
	return nil
}
func (f *fakeOrch) DeleteCluster(_ context.Context, n string) error {
	if _, ok := f.clusters[n]; !ok {
		return model.ErrNotFound
	}
	f.deleted = append(f.deleted, n)
	return nil
}
func (f *fakeOrch) HostStatus(_ context.Context, id string) (domain.Host, error) {
	if id != "gpu-node-0001" {
		return domain.Host{}, model.ErrNotFound
	}
	return domain.Host{ID: id, State: domain.StateReady}, nil
}
func (f *fakeOrch) DecommissionHost(_ context.Context, id string) error {
	f.decom = append(f.decom, id)
	return nil
}

func newServer() (*Server, *fakeOrch, *events.MemoryBus) {
	f, bus := newFake(), events.NewMemoryBus()
	return &Server{Orch: f, Bus: bus, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, f, bus
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

const manifest = `{"metadata":{"name":"llama-prod"},"spec":{"gpus":32,"topology":{"interconnect":"infiniband"}}}`

func TestCreateClusterIsOneCallAndIdempotent(t *testing.T) {
	s, f, _ := newServer()
	rec := do(t, s, "POST", "/v1/clusters", manifest)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d body=%s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/clusters/llama-prod" {
		t.Fatalf("location = %q", loc)
	}
	if got := f.clusters["llama-prod"].DesiredHosts; got != 4 {
		t.Fatalf("desired hosts = %d, want 4", got)
	}
	rec = do(t, s, "POST", "/v1/clusters", manifest)
	if rec.Code != http.StatusOK || f.startedOK != 1 {
		t.Fatalf("re-apply: code=%d starts=%d", rec.Code, f.startedOK)
	}
}

func TestCreateClusterValidation(t *testing.T) {
	s, _, _ := newServer()
	cases := map[string]struct {
		body string
		code int
	}{
		"bad json":      {`{`, 400},
		"unknown field": {`{"metadata":{"name":"a"},"spec":{"gpus":8,"bogus":1}}`, 400},
		"not multiple":  {`{"metadata":{"name":"a"},"spec":{"gpus":12}}`, 422},
		"bad name":      {`{"metadata":{"name":"A_B"},"spec":{"gpus":8}}`, 422},
	}
	for name, c := range cases {
		if rec := do(t, s, "POST", "/v1/clusters", c.body); rec.Code != c.code {
			t.Errorf("%s: code = %d, want %d (%s)", name, rec.Code, c.code, rec.Body)
		}
	}
}

func TestScaleViaPut(t *testing.T) {
	s, f, _ := newServer()
	do(t, s, "POST", "/v1/clusters", manifest)

	up := strings.Replace(manifest, `"gpus":32`, `"gpus":64`, 1)
	if rec := do(t, s, "PUT", "/v1/clusters/llama-prod", up); rec.Code != 202 || f.scaled["llama-prod"] != 4 {
		t.Fatalf("scale up: code=%d delta=%d", rec.Code, f.scaled["llama-prod"])
	}
	down := strings.Replace(manifest, `"gpus":32`, `"gpus":16`, 1)
	if rec := do(t, s, "PUT", "/v1/clusters/llama-prod", down); rec.Code != 202 || f.scaled["llama-prod"] != -2 {
		t.Fatalf("scale down: delta=%d", f.scaled["llama-prod"])
	}
	if rec := do(t, s, "PUT", "/v1/clusters/llama-prod", manifest); rec.Code != 200 {
		t.Fatalf("no-op put: code=%d", rec.Code)
	}
}

func TestPutRejectsImmutableChangesAndNameMismatch(t *testing.T) {
	s, _, _ := newServer()
	do(t, s, "POST", "/v1/clusters", manifest)
	changed := strings.Replace(manifest, `"gpus":32`, `"gpus":32,"driverVersion":"999"`, 1)
	rec := do(t, s, "PUT", "/v1/clusters/llama-prod", changed)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "spec.driverVersion") {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if rec := do(t, s, "PUT", "/v1/clusters/other", manifest); rec.Code != 422 {
		t.Fatalf("name mismatch: code=%d", rec.Code)
	}
	if rec := do(t, s, "PUT", "/v1/clusters/ghost", strings.Replace(manifest, "llama-prod", "ghost", 1)); rec.Code != 404 {
		t.Fatalf("missing: code=%d", rec.Code)
	}
}

func TestGetAndDelete(t *testing.T) {
	s, f, _ := newServer()
	do(t, s, "POST", "/v1/clusters", manifest)

	rec := do(t, s, "GET", "/v1/clusters/llama-prod", "")
	var st model.ClusterStatus
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &st) != nil || st.Phase != model.PhaseProvisioning {
		t.Fatalf("get: code=%d body=%s", rec.Code, rec.Body)
	}
	if rec := do(t, s, "GET", "/v1/clusters/nope", ""); rec.Code != 404 {
		t.Fatalf("get missing: %d", rec.Code)
	}
	if rec := do(t, s, "DELETE", "/v1/clusters/llama-prod", ""); rec.Code != 202 || len(f.deleted) != 1 {
		t.Fatalf("delete: code=%d", rec.Code)
	}
	if rec := do(t, s, "DELETE", "/v1/clusters/nope", ""); rec.Code != 404 {
		t.Fatalf("delete missing: %d", rec.Code)
	}
}

func TestHostEndpoints(t *testing.T) {
	s, f, _ := newServer()
	if rec := do(t, s, "GET", "/v1/hosts/gpu-node-0001", ""); rec.Code != 200 {
		t.Fatalf("get host: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/v1/hosts/nope", ""); rec.Code != 404 {
		t.Fatalf("get missing host: %d", rec.Code)
	}
	if rec := do(t, s, "DELETE", "/v1/hosts/gpu-node-0001", ""); rec.Code != 202 || f.decom[0] != "gpu-node-0001" {
		t.Fatalf("decommission: %d", rec.Code)
	}
}

func TestHealthEventIsPublishedToBus(t *testing.T) {
	s, _, bus := newServer()
	rec := do(t, s, "POST", "/v1/hosts/gpu-node-0001/health-events", `{"reason":"xid 79: GPU fell off the bus"}`)
	if rec.Code != 202 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	evs := bus.Published()
	if len(evs) != 1 || evs[0].Type != events.TypeHealthDegraded || evs[0].HostID != "gpu-node-0001" ||
		evs[0].Reason != "xid 79: GPU fell off the bus" {
		t.Fatalf("events = %+v", evs)
	}
	// Empty body is allowed and gets a default reason.
	rec = do(t, s, "POST", "/v1/hosts/gpu-node-0002/health-events", "")
	if rec.Code != 202 || bus.Published()[1].Reason == "" {
		t.Fatalf("empty body: code=%d", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	s, _, _ := newServer()
	if rec := do(t, s, "GET", "/healthz", ""); rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
}
