package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCommandVerbose(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	path := filepath.Join(t.TempDir(), "scenario.yaml")
	config := fmt.Sprintf(`name: verbosity-test
streams:
  - name: telemetry
    protocol: udp
    target: %s
    rate: 1
    payload: test
events:
  - at: 0s
    stream: telemetry
    action: stop
`, listener.LocalAddr())
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}

	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("verbose=%t", verbose), func(t *testing.T) {
			stdout, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			defer stdout.Close()
			stderr, err := os.CreateTemp(t.TempDir(), "stderr")
			if err != nil {
				t.Fatal(err)
			}
			defer stderr.Close()
			originalOut, originalErr := os.Stdout, os.Stderr
			os.Stdout, os.Stderr = stdout, stderr
			defer func() { os.Stdout, os.Stderr = originalOut, originalErr }()

			args := []string{path}
			if verbose {
				args = []string{"--verbose", path}
			}
			if err := runCommand(args); err != nil {
				t.Fatal(err)
			}
			out, err := os.ReadFile(stdout.Name())
			if err != nil {
				t.Fatal(err)
			}
			diagnostics, err := os.ReadFile(stderr.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), "[SUMMARY: telemetry]") {
				t.Fatalf("missing stream summary: %s", out)
			}
			if strings.Contains(string(out), "[SCHEDULER]") || strings.Contains(string(out), "\x1b[") {
				t.Fatalf("redirected stdout contains diagnostics or colors: %q", out)
			}
			if got := strings.Contains(string(diagnostics), "[SCHEDULER]"); got != verbose {
				t.Fatalf("verbose=%t, stderr=%q", verbose, diagnostics)
			}
		})
	}
}

func TestRunCommandRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"--verbose"}, {"one.yaml", "two.yaml"}, {"--unknown"}} {
		if err := runCommand(args); err == nil {
			t.Fatalf("runCommand(%q) accepted invalid arguments", args)
		}
	}
}
