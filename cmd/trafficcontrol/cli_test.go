package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestCLIProcessHelper exercises main in a subprocess so exit codes are observed.
func TestCLIProcessHelper(t *testing.T) {
	if os.Getenv("TRAFFICCONTROL_CLI_TEST_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = os.Args[i:]
			main()
			os.Exit(0)
		}
	}
	t.Fatal("missing subprocess argument separator")
}

func TestCLIHelpAndErrors(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		exit   int
		stdout []string
		stderr []string
	}{
		{name: "root help", args: []string{"--help"}, stdout: []string{"Usage: trafficcontrol <command>", "run", "listen", "help", "--version"}},
		{name: "short root help", args: []string{"-h"}, stdout: []string{"Usage: trafficcontrol <command>"}},
		{name: "help command", args: []string{"help"}, stdout: []string{"Usage: trafficcontrol <command>"}},
		{name: "run help", args: []string{"run", "--help"}, stdout: []string{"Usage: trafficcontrol run [--verbose] <scenario.yaml>", "before the scenario path", "-verbose", "Examples:"}},
		{name: "short run help", args: []string{"run", "-h"}, stdout: []string{"Usage: trafficcontrol run"}},
		{name: "help run", args: []string{"help", "run"}, stdout: []string{"Usage: trafficcontrol run"}},
		{name: "listen help", args: []string{"listen", "--help"}, stdout: []string{"Usage: trafficcontrol listen", "-protocol", "-address", "Examples:", "tcp", "udp"}},
		{name: "help listen", args: []string{"help", "listen"}, stdout: []string{"Usage: trafficcontrol listen"}},
		{name: "version", args: []string{"--version"}, stdout: []string{"trafficcontrol dev"}},
		{name: "no command", exit: 1, stderr: []string{"Usage: trafficcontrol <command>"}},
		{name: "unknown command", args: []string{"bogus"}, exit: 1, stderr: []string{"'bogus' is not a trafficcontrol command", "trafficcontrol --help"}},
		{name: "unknown help topic", args: []string{"help", "bogus"}, exit: 1, stderr: []string{"unknown help topic", "trafficcontrol --help"}},
		{name: "extra help argument", args: []string{"help", "run", "extra"}, exit: 1, stderr: []string{"trafficcontrol help [command]"}},
		{name: "extra root help argument", args: []string{"--help", "run"}, exit: 1, stderr: []string{"trafficcontrol help [command]"}},
		{name: "extra version argument", args: []string{"--version", "extra"}, exit: 1, stderr: []string{"usage: trafficcontrol --version"}},
		{name: "missing scenario", args: []string{"run"}, exit: 1, stderr: []string{"usage: trafficcontrol run", "trafficcontrol run --help"}},
		{name: "unknown run flag", args: []string{"run", "--unknown"}, exit: 1, stderr: []string{"flag provided but not defined: -unknown", "trafficcontrol run --help"}},
		{name: "invalid boolean", args: []string{"run", "--verbose=maybe"}, exit: 1, stderr: []string{"invalid boolean value", "trafficcontrol run --help"}},
		{name: "flag after scenario", args: []string{"run", "missing.yaml", "--verbose"}, exit: 1, stderr: []string{"flags must appear before the scenario path"}},
		{name: "unknown listen flag", args: []string{"listen", "--unknown"}, exit: 1, stderr: []string{"flag provided but not defined: -unknown", "trafficcontrol listen --help"}},
		{name: "missing flag value", args: []string{"listen", "--protocol"}, exit: 1, stderr: []string{"flag needs an argument: -protocol", "trafficcontrol listen --help"}},
		{name: "unsupported protocol", args: []string{"listen", "--protocol", "sctp"}, exit: 1, stderr: []string{"unsupported receiver protocol"}},
		{name: "unexpected listen argument", args: []string{"listen", "extra"}, exit: 1, stderr: []string{"usage: trafficcontrol listen", "trafficcontrol listen --help"}},
		{name: "missing scenario file", args: []string{"run", "missing.yaml"}, exit: 1, stderr: []string{"error reading scenario config file"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			args := append([]string{"-test.run=^TestCLIProcessHelper$", "--"}, tt.args...)
			cmd := exec.CommandContext(ctx, os.Args[0], args...)
			cmd.Env = append(os.Environ(), "TRAFFICCONTROL_CLI_TEST_PROCESS=1")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			exit := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatal(err)
				}
				exit = exitErr.ExitCode()
			}
			if exit != tt.exit {
				t.Fatalf("exit=%d, want %d; stdout=%q stderr=%q", exit, tt.exit, stdout.String(), stderr.String())
			}
			if len(tt.stdout) == 0 && stdout.Len() != 0 {
				t.Errorf("unexpected stdout: %q", stdout.String())
			}
			if len(tt.stderr) == 0 && stderr.Len() != 0 {
				t.Errorf("unexpected stderr: %q", stderr.String())
			}
			for _, want := range tt.stdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout=%q, missing %q", stdout.String(), want)
				}
			}
			for _, want := range tt.stderr {
				if count := strings.Count(stderr.String(), want); count != 1 {
					t.Errorf("stderr=%q, want %q exactly once, got %d", stderr.String(), want, count)
				}
			}
		})
	}
}
