package main

import (
	"bytes"
	"testing"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

func TestConsoleReporterFormatsUpdates(t *testing.T) {
	snapshot := engine.StreamSnapshot{
		Name:        "telemetry",
		Status:      engine.StreamStatusRunning,
		TargetRate:  25,
		PacketsSent: 10,
		BytesSent:   50,
		LastError:   "send failed",
	}

	tests := []struct {
		name string
		kind engine.UpdateKind
		want string
	}{
		{
			name: "started",
			kind: engine.UpdateStarted,
			want: "[STARTED: telemetry] rate=25 pkt/s\n",
		},
		{
			name: "snapshot",
			kind: engine.UpdateSnapshot,
			want: "[SNAPSHOT: telemetry] status=running rate=25 pkt/s packets=10 bytes=50\n",
		},
		{
			name: "rate changed",
			kind: engine.UpdateRateChanged,
			want: "[RATE_CHANGED: telemetry] rate=25 pkt/s status=running\n",
		},
		{
			name: "paused",
			kind: engine.UpdatePaused,
			want: "[PAUSED: telemetry] packets=10 bytes=50\n",
		},
		{
			name: "resumed",
			kind: engine.UpdateResumed,
			want: "[RESUMED: telemetry] rate=25 pkt/s\n",
		},
		{
			name: "stopped",
			kind: engine.UpdateStopped,
			want: "[STOPPED: telemetry] packets=10 bytes=50\n",
		},
		{
			name: "canceled",
			kind: engine.UpdateCanceled,
			want: "[CANCELED: telemetry] packets=10 bytes=50\n",
		},
		{
			name: "failed",
			kind: engine.UpdateFailed,
			want: "[FAILED: telemetry] error=\"send failed\" packets=10 bytes=50\n",
		},
		{
			name: "unknown",
			kind: engine.UpdateKind("unknown"),
			want: "[UNKNOWN: telemetry] status=running\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			reporter := newConsoleReporter(&output)

			reporter.Report(engine.StreamUpdate{
				Kind:     tt.kind,
				Snapshot: snapshot,
			})

			if got := output.String(); got != tt.want {
				t.Fatalf("output = %q, want %q", got, tt.want)
			}
		})
	}
}
