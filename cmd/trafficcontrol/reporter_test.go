package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

func TestConsoleReporterFormatsUpdates(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	snapshot := engine.StreamSnapshot{
		Name:        "telemetry",
		Status:      engine.StreamStatusRunning,
		TargetRate:  25,
		PacketsSent: 10,
		BytesSent:   50,
		StartedAt:   startedAt,
		CapturedAt:  startedAt.Add(2 * time.Second),
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
			want: "[SNAPSHOT: telemetry] status=running target=25 sends/s actual=0.0 sends/s sends=10 bytes=50\n",
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
			want: "[STOPPED: telemetry] packets=10 bytes=50\n" +
				"[SUMMARY: telemetry] reason=stopped duration=2.000s target=25 packets/s packets=10 bytes=50 average=5.0 packets/s\n",
		},
		{
			name: "canceled",
			kind: engine.UpdateCanceled,
			want: "[CANCELED: telemetry] packets=10 bytes=50\n" +
				"[SUMMARY: telemetry] reason=canceled duration=2.000s target=25 packets/s packets=10 bytes=50 average=5.0 packets/s\n",
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

func TestConsoleReporterFormatsBenchmarkSummary(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	reporter := newConsoleReporter(&output)

	reporter.Report(engine.StreamUpdate{
		Kind: engine.UpdateStopped,
		Snapshot: engine.StreamSnapshot{
			Name:        "benchmark",
			Status:      engine.StreamStatusStopped,
			TargetRate:  50000,
			PacketsSent: 499847,
			BytesSent:   4498623,
			StartedAt:   startedAt,
			CapturedAt:  startedAt.Add(10 * time.Second),
		},
	})

	want := "[STOPPED: benchmark] packets=499847 bytes=4498623\n" +
		"[SUMMARY: benchmark] reason=stopped duration=10.000s target=50000 packets/s packets=499847 bytes=4498623 average=49984.7 packets/s\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}
