// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"sigs.k8s.io/agent-sandbox/sandbox-router/config"
)

// freeAddr returns a loopback address with a port nothing is listening on.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// newSlowAPIServer returns an API server whose pod LIST blocks until release is
// closed, then fails with 500.
func newSlowAPIServer(release <-chan struct{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		http.Error(w, "slow", http.StatusInternalServerError)
	}))
}

// testConfig writes a kubeconfig pointing at apiURL and returns a config that
// enables the pod cache and binds every listener to a free loopback port.
func testConfig(t *testing.T, apiURL string) config.Config {
	t.Helper()
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	kc := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: %s
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: test
`, apiURL)
	if err := os.WriteFile(kubeconfig, []byte(kc), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	cfg := config.Defaults()
	cfg.HTTPAddr = freeAddr(t)
	cfg.MetricsAddr = freeAddr(t)
	cfg.HTTPSAddr = ""
	cfg.ProbeAddr = freeAddr(t)
	cfg.CacheEnabled = true
	cfg.CacheNamespace = "default"
	cfg.Kubeconfig = kubeconfig
	return cfg
}

// waitRun waits for run() to return, failing the test if it takes longer than
// the bound, so a hang shows up as a clear failure.
func waitRun(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("run() did not return within 30s")
	}
}

// TestRun_HealthzAnswersDuringSlowCacheSync pins the fix: while the initial
// pod LIST is still pending, /healthz must return 200 (so the kubelet does not
// restart the pod) and /readyz must return 503.
func TestRun_HealthzAnswersDuringSlowCacheSync(t *testing.T) {
	release := make(chan struct{})
	api := newSlowAPIServer(release)
	defer api.Close()
	// Registered after api.Close, so it runs first: unblocks the pending LIST
	// so run() returns before the test server shuts down.
	defer close(release)

	cfg := testConfig(t, api.URL)
	probeAddr := cfg.ProbeAddr

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = run(ctx, &cfg, logr.Discard())
	}()
	// Cancel run and wait for it to exit, even when an assertion fails, so
	// the informer and probe listener do not outlive the test.
	t.Cleanup(func() {
		cancel()
		<-done
	})

	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	get := func(path string) (int, error) {
		resp, err := client.Get("http://" + probeAddr + path)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		return resp.StatusCode, nil
	}

	// Wait for the probe listener to come up, then check both probes while
	// the sync is still blocked.
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			t.Fatalf("run() returned before the probes were checked")
		default:
		}
		code, err := get("/healthz")
		if err == nil {
			if code != http.StatusOK {
				t.Fatalf("healthz during sync: got %d want 200", code)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe listener never came up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	code, err := get("/readyz")
	if err != nil {
		t.Fatalf("readyz get: %v", err)
	}
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readyz during sync: got %d want 503", code)
	}

	// Cancelling during the sync should report the shutdown, not an RBAC problem.
	cancel()
	waitRun(t, done)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run() after cancel: got %v, want context.Canceled", runErr)
	}
}

// TestRun_SyncTimeoutReportsRBACHint covers the other branch of the sync
// failure: the sync times out while run() is not cancelled, so the error must
// point at RBAC rather than at shutdown.
func TestRun_SyncTimeoutReportsRBACHint(t *testing.T) {
	prev := cacheSyncTimeout
	cacheSyncTimeout = 300 * time.Millisecond
	t.Cleanup(func() { cacheSyncTimeout = prev })

	release := make(chan struct{})
	api := newSlowAPIServer(release)
	defer api.Close()
	defer close(release)

	cfg := testConfig(t, api.URL)

	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = run(t.Context(), &cfg, logr.Discard())
	}()
	waitRun(t, done)

	if runErr == nil {
		t.Fatal("run() returned nil, want the sync failure")
	}
	if !strings.Contains(runErr.Error(), "check RBAC") {
		t.Fatalf("run() error: got %q, want the RBAC hint", runErr)
	}
	if strings.Contains(runErr.Error(), "shutdown") {
		t.Fatalf("run() error %q wrongly reports shutdown", runErr)
	}
}
