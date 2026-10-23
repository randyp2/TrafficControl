package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

func TestConsoleColorDisablesRedirectedOutput(t *testing.T) {
	var output bytes.Buffer
	if consoleColor(&output) {
		t.Fatal("enabled color for a buffer")
	}
	file, err := os.CreateTemp(t.TempDir(), "output")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if consoleColor(file) {
		t.Fatal("enabled color for a redirected file")
	}
}

func TestConsoleColorRespectsEnvironment(t *testing.T) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, name := range []string{"NO_COLOR", "TERM"} {
		t.Run(name, func(t *testing.T) {
			if name == "NO_COLOR" {
				t.Setenv(name, "1")
			} else {
				t.Setenv(name, "dumb")
			}
			if consoleColor(file) {
				t.Fatalf("ignored %s", name)
			}
		})
	}
}

type recordingWriter struct {
	bytes.Buffer
	writes int
}

func (w *recordingWriter) WriteString(text string) (int, error) {
	w.writes++
	return w.Buffer.WriteString(text)
}

func TestConsoleReporterWritesCompleteColoredUpdates(t *testing.T) {
	for _, tt := range []struct {
		kind  engine.UpdateKind
		color string
	}{
		{engine.UpdateStarted, colorGreen},
		{engine.UpdateLossChanged, colorYellow},
		{engine.UpdateFailed, colorRed},
	} {
		t.Run(string(tt.kind), func(t *testing.T) {
			var output recordingWriter
			reporter := newConsoleReporter(&output)
			reporter.color = true
			reporter.Report(engine.StreamUpdate{
				Kind: tt.kind,
				Snapshot: engine.StreamSnapshot{
					Name: "telemetry",
				},
			})
			label := "[" + strings.ToUpper(string(tt.kind)) + ": telemetry]"
			if !strings.Contains(output.String(), tt.color+label+"\x1b[0m") {
				t.Fatalf("missing colored label: %q", output.String())
			}
			if output.writes != 1 {
				t.Fatalf("wrote %d fragments, want one complete update", output.writes)
			}
		})
	}
}
