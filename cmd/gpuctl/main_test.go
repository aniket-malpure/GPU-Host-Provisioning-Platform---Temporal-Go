package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyCreatesThenScalesWhenExisting(t *testing.T) {
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), "llama-prod") {
			t.Errorf("manifest not forwarded: %s", b)
		}
		if r.Method == "POST" {
			w.WriteHeader(http.StatusOK) // 200 == already exists
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	f := filepath.Join(t.TempDir(), "m.json")
	_ = os.WriteFile(f, []byte(`{"metadata":{"name":"llama-prod"},"spec":{"gpus":16}}`), 0o600)

	var out bytes.Buffer
	if err := run([]string{"--server", srv.URL, "apply", "-f", f}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "POST /v1/clusters,PUT /v1/clusters/llama-prod" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestErrorsSurfaceServerMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}))
	defer srv.Close()
	err := run([]string{"--server", srv.URL, "get", "cluster", "nope"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v", err)
	}
}

func TestResourceRouting(t *testing.T) {
	if resource("host", "h1") != "/v1/hosts/h1" || resource("cluster", "c") != "/v1/clusters/c" {
		t.Fatal("bad routing")
	}
}

func TestUsage(t *testing.T) {
	if err := run(nil, io.Discard); err == nil {
		t.Fatal("expected usage error")
	}
	if err := run([]string{"apply"}, io.Discard); err == nil {
		t.Fatal("apply without -f must fail")
	}
}
