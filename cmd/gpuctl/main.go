// gpuctl is the self-service CLI for the GPU provisioning platform.
//
//	gpuctl apply -f cluster.json        create or scale a cluster from a manifest
//	gpuctl get cluster NAME             show cluster status
//	gpuctl get host ID                  show a host and its state history
//	gpuctl wait cluster NAME [-timeout 10m]
//	gpuctl delete cluster NAME          tear a cluster down
//	gpuctl delete host ID               decommission one host
//	gpuctl report-degraded HOST [reason...]
//
// The server defaults to $GPUCTL_SERVER or http://localhost:8080.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type client struct {
	base string
	http *http.Client
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	server := os.Getenv("GPUCTL_SERVER")
	if server == "" {
		server = "http://localhost:8080"
	}
	if len(args) >= 2 && args[0] == "--server" {
		server, args = args[1], args[2:]
	}
	if len(args) == 0 {
		return errors.New(usage)
	}
	c := &client{base: strings.TrimRight(server, "/"), http: &http.Client{Timeout: 30 * time.Second}}

	switch args[0] {
	case "apply":
		fs := flag.NewFlagSet("apply", flag.ContinueOnError)
		file := fs.String("f", "", "manifest file (JSON)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *file == "" {
			return errors.New("apply: -f <manifest.json> is required")
		}
		return c.apply(*file, out)
	case "get":
		if len(args) != 3 {
			return errors.New("usage: gpuctl get cluster|host NAME")
		}
		return c.show(resource(args[1], args[2]), out)
	case "delete":
		if len(args) != 3 {
			return errors.New("usage: gpuctl delete cluster|host NAME")
		}
		_, err := c.do("DELETE", resource(args[1], args[2]), nil, out)
		return err
	case "wait":
		fs := flag.NewFlagSet("wait", flag.ContinueOnError)
		timeout := fs.Duration("timeout", 10*time.Minute, "give up after")
		if len(args) < 3 || args[1] != "cluster" {
			return errors.New("usage: gpuctl wait cluster NAME [-timeout 10m]")
		}
		if err := fs.Parse(args[3:]); err != nil {
			return err
		}
		return c.waitReady(args[2], *timeout, out)
	case "report-degraded":
		if len(args) < 2 {
			return errors.New("usage: gpuctl report-degraded HOST [reason...]")
		}
		body, _ := json.Marshal(map[string]string{"reason": strings.Join(args[2:], " ")})
		_, err := c.do("POST", "/v1/hosts/"+args[1]+"/health-events", body, out)
		return err
	default:
		return errors.New(usage)
	}
}

const usage = "usage: gpuctl [--server URL] apply|get|delete|wait|report-degraded ..."

func resource(kind, name string) string {
	switch kind {
	case "host", "hosts":
		return "/v1/hosts/" + name
	default:
		return "/v1/clusters/" + name
	}
}

func (c *client) apply(file string, out io.Writer) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var m struct {
		Metadata struct{ Name string } `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || m.Metadata.Name == "" {
		return errors.New("manifest must be JSON with metadata.name")
	}
	code, err := c.do("POST", "/v1/clusters", raw, out)
	if err != nil {
		return err
	}
	if code == http.StatusOK { // already exists: reconcile (scale) instead
		_, err = c.do("PUT", "/v1/clusters/"+m.Metadata.Name, raw, out)
	}
	return err
}

func (c *client) show(path string, out io.Writer) error {
	_, err := c.do("GET", path, nil, out)
	return err
}

func (c *client) waitReady(name string, timeout time.Duration, out io.Writer) error {
	deadline := time.Now().Add(timeout)
	for {
		var buf bytes.Buffer
		if _, err := c.do("GET", "/v1/clusters/"+name, nil, &buf); err != nil {
			return err
		}
		var st struct {
			Phase string `json:"phase"`
		}
		_ = json.Unmarshal(buf.Bytes(), &st)
		if st.Phase == "Ready" {
			fmt.Fprint(out, buf.String())
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s (phase %s)", name, st.Phase)
		}
		time.Sleep(2 * time.Second)
	}
}

// do performs a request, writes the pretty-printed body to out and returns the
// status code. Non-2xx statuses are returned as errors.
func (c *client) do(method, path string, body []byte, out io.Writer) (int, error) {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, data, "", "  ") == nil {
		data = pretty.Bytes()
	}
	fmt.Fprintln(out, strings.TrimSpace(string(data)))
	return resp.StatusCode, nil
}
