package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

const toxiproxyAPIAddr = "127.0.0.1:18474"

// toxiproxyServer manages a toxiproxy-server subprocess.
type toxiproxyServer struct {
	cmd  *exec.Cmd
	api  string
}

// startToxiproxy starts a toxiproxy-server if the binary is in PATH.
// Returns nil and skips the test if not found.
func startToxiproxy(t *testing.T) *toxiproxyServer {
	t.Helper()

	path, err := exec.LookPath("toxiproxy-server")
	if err != nil {
		t.Skip("toxiproxy-server not in PATH, skipping chaos tests")
		return nil
	}

	// Find a free port for the API.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	apiAddr := lis.Addr().String()
	lis.Close()

	cmd := exec.Command(path, "-host", "127.0.0.1", "-port", portOf(apiAddr))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}

	srv := &toxiproxyServer{cmd: cmd, api: apiAddr}

	// Wait for the API to be ready.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/version", apiAddr))
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	return srv
}

type proxyConfig struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
}

// createProxy creates a toxiproxy proxy.
func (s *toxiproxyServer) createProxy(t *testing.T, name, listen, upstream string) {
	t.Helper()
	cfg := proxyConfig{Name: name, Listen: listen, Upstream: upstream}
	body, _ := json.Marshal(cfg)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/proxies", s.api),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create proxy %s: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create proxy %s: status %d", name, resp.StatusCode)
	}
}

// disableProxy disables a proxy (drops all traffic).
func (s *toxiproxyServer) disableProxy(t *testing.T, name string) {
	t.Helper()
	s.setProxyEnabled(t, name, false)
}

// enableProxy re-enables a proxy.
func (s *toxiproxyServer) enableProxy(t *testing.T, name string) {
	t.Helper()
	s.setProxyEnabled(t, name, true)
}

func (s *toxiproxyServer) setProxyEnabled(t *testing.T, name string, enabled bool) {
	t.Helper()
	body, _ := json.Marshal(map[string]bool{"enabled": enabled})
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/proxies/%s", s.api, name),
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("toggle proxy %s: %v", name, err)
	}
	resp.Body.Close()
}

type toxicConfig struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Stream     string            `json:"stream"`
	Toxicity   float64           `json:"toxicity"`
	Attributes map[string]int    `json:"attributes"`
}

// addLatencyToxic adds latency to a proxy.
func (s *toxiproxyServer) addLatencyToxic(t *testing.T, proxyName string, latencyMs, jitterMs int) {
	t.Helper()
	toxic := toxicConfig{
		Name:     proxyName + "-latency",
		Type:     "latency",
		Stream:   "upstream",
		Toxicity: 1.0,
		Attributes: map[string]int{
			"latency": latencyMs,
			"jitter":  jitterMs,
		},
	}
	body, _ := json.Marshal(toxic)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/proxies/%s/toxics", s.api, proxyName),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("add toxic to %s: %v", proxyName, err)
	}
	resp.Body.Close()
}

// removeToxic removes a toxic by name.
func (s *toxiproxyServer) removeToxic(t *testing.T, proxyName, toxicName string) {
	t.Helper()
	req, _ := http.NewRequest("DELETE",
		fmt.Sprintf("http://%s/proxies/%s/toxics/%s", s.api, proxyName, toxicName),
		nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("remove toxic %s: %v", toxicName, err)
	}
	resp.Body.Close()
}

// portOf extracts the port from a "host:port" string.
func portOf(addr string) string {
	_, port, _ := net.SplitHostPort(addr)
	return port
}
