// Package api is the self-service surface: a declarative REST API. The
// inference team submits a manifest and the platform does the rest, with no
// ticket and no human in the loop.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aniketmalpure/gpu-host-provisioner/internal/domain"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/events"
	"github.com/aniketmalpure/gpu-host-provisioner/internal/model"
)

// Orchestrator is the control-plane backend (Temporal in production, a fake
// in tests). The API never needs to know which engine runs the workflows.
type Orchestrator interface {
	// StartCluster starts the cluster workflow. created is false when a
	// cluster with that name is already running (idempotent apply).
	StartCluster(ctx context.Context, m model.Manifest) (created bool, err error)
	ClusterStatus(ctx context.Context, name string) (model.ClusterStatus, error)
	ScaleCluster(ctx context.Context, name string, deltaHosts int) error
	DeleteCluster(ctx context.Context, name string) error
	HostStatus(ctx context.Context, hostID string) (domain.Host, error)
	DecommissionHost(ctx context.Context, hostID string) error
}

// Server wires HTTP routes to the orchestrator and event bus.
type Server struct {
	Orch Orchestrator
	Bus  events.Bus
	Log  *slog.Logger
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/clusters", s.createCluster)
	mux.HandleFunc("PUT /v1/clusters/{name}", s.updateCluster)
	mux.HandleFunc("GET /v1/clusters/{name}", s.getCluster)
	mux.HandleFunc("DELETE /v1/clusters/{name}", s.deleteCluster)
	mux.HandleFunc("GET /v1/hosts/{id}", s.getHost)
	mux.HandleFunc("DELETE /v1/hosts/{id}", s.decommissionHost)
	mux.HandleFunc("POST /v1/hosts/{id}/health-events", s.reportHealth)
	return mux
}

func (s *Server) createCluster(w http.ResponseWriter, r *http.Request) {
	m, ok := decodeManifest(w, r)
	if !ok {
		return
	}
	created, err := s.Orch.StartCluster(r.Context(), m)
	if err != nil {
		s.fail(w, err)
		return
	}
	loc := "/v1/clusters/" + m.Metadata.Name
	w.Header().Set("Location", loc)
	code := http.StatusAccepted // 202: reconciliation continues in the background
	if !created {
		code = http.StatusOK // idempotent re-apply of an existing cluster
	}
	writeJSON(w, code, map[string]any{"name": m.Metadata.Name, "created": created, "status": loc, "hosts": m.Spec.Hosts()})
}

func (s *Server) updateCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	m, ok := decodeManifest(w, r)
	if !ok {
		return
	}
	if m.Metadata.Name != name {
		writeErr(w, http.StatusUnprocessableEntity, "metadata.name must match the URL")
		return
	}
	cur, err := s.Orch.ClusterStatus(r.Context(), name)
	if err != nil {
		s.fail(w, err)
		return
	}
	if diff := model.ImmutableDiff(cur.Spec, m.Spec); len(diff) > 0 {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("immutable fields changed: %s", strings.Join(diff, ", ")))
		return
	}
	delta := m.Spec.Hosts() - cur.DesiredHosts
	if delta == 0 {
		writeJSON(w, http.StatusOK, cur)
		return
	}
	if err := s.Orch.ScaleCluster(r.Context(), name, delta); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"name": name, "deltaHosts": delta})
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	st, err := s.Orch.ClusterStatus(r.Context(), r.PathValue("name"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) deleteCluster(w http.ResponseWriter, r *http.Request) {
	if err := s.Orch.DeleteCluster(r.Context(), r.PathValue("name")); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "terminating"})
}

func (s *Server) getHost(w http.ResponseWriter, r *http.Request) {
	h, err := s.Orch.HostStatus(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) decommissionHost(w http.ResponseWriter, r *http.Request) {
	if err := s.Orch.DecommissionHost(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "decommissioning"})
}

// reportHealth ingests a health signal from a node agent or monitor and
// publishes it to the event stream; the remediator reacts to it.
func (s *Server) reportHealth(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Reason == "" {
		body.Reason = "reported via API"
	}
	id := r.PathValue("id")
	now := time.Now().UTC()
	ev := events.Event{
		ID:     fmt.Sprintf("%s:health:%d", id, now.UnixNano()),
		Type:   events.TypeHealthDegraded,
		HostID: id, Reason: body.Reason, Time: now,
	}
	if err := s.Bus.Publish(r.Context(), ev); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"eventId": ev.ID})
}

func decodeManifest(w http.ResponseWriter, r *http.Request) (model.Manifest, bool) {
	var m model.Manifest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid manifest: "+err.Error())
		return m, false
	}
	m = m.Normalize()
	if err := m.Validate(); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return m, false
	}
	return m, true
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, model.ErrAlreadyExists):
		writeErr(w, http.StatusConflict, err.Error())
	default:
		s.Log.Error("request failed", "error", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
