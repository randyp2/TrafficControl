package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

func TestConsoleReporterFormatsUpdates(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	snapshot := engine.StreamSnapshot{
		Name:             "telemetry",
		Status:           engine.StreamStatusRunning,
		TargetRate:       25,
		LossPercent:      50,
		Jitter:           10 * time.Millisecond,
		PacketsGenerated: 20,
		PacketsDropped:   10,
		PacketsSent:      10,
		BytesSent:        50,
		StartedAt:        startedAt,
		CapturedAt:       startedAt.Add(2 * time.Second),
		LastError:        "send failed",
	}

	tests := []struct {
		name string
		kind engine.UpdateKind
		want string
	}{
		{
			name: "started",
			kind: engine.UpdateStarted,
			want: "   2.00s [STARTED: telemetry] target=25 packets/s",
		},
		{
			name: "snapshot",
			kind: engine.UpdateSnapshot,
			want: "2.00s telemetry running 25 0.0 0.0 10 50.00 0s",
		},
		{
			name: "rate changed",
			kind: engine.UpdateRateChanged,
			want: "   2.00s [RATE_CHANGED: telemetry] target=25 packets/s status=running",
		},
		{
			name: "jitter changed",
			kind: engine.UpdateJitterChanged,
			want: "   2.00s [JITTER_CHANGED: telemetry] jitter=+/- 10ms status=running",
		},
		{
			name: "loss changed",
			kind: engine.UpdateLossChanged,
			want: "   2.00s [PACKET_LOSS_CHANGED: telemetry] loss=50.00% status=running",
		},
		{
			name: "paused",
			kind: engine.UpdatePaused,
			want: "   2.00s [PAUSED: telemetry] sent=10 bytes=50",
		},
		{
			name: "resumed",
			kind: engine.UpdateResumed,
			want: "   2.00s [RESUMED: telemetry] target=25 packets/s",
		},
		{
			name: "stopped",
			kind: engine.UpdateStopped,
			want: "   2.00s [STOPPED: telemetry] sent=10 bytes=50",
		},
		{
			name: "canceled",
			kind: engine.UpdateCanceled,
			want: "   2.00s [CANCELED: telemetry] sent=10 bytes=50",
		},
		{
			name: "failed",
			kind: engine.UpdateFailed,
			want: "   2.00s [FAILED: telemetry] error=\"send failed\" sent=10 bytes=50",
		},
		{
			name: "unknown",
			kind: engine.UpdateKind("unknown"),
			want: "   2.00s [UNKNOWN: telemetry] status=running",
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

			got := strings.SplitN(output.String(), "\n", 2)[0]
			if tt.kind == engine.UpdateSnapshot {
				lines := strings.Split(strings.TrimSpace(output.String()), "\n")
				got = strings.Join(strings.Fields(lines[len(lines)-1]), " ")
			}
			if got != tt.want {
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
			Name:             "benchmark",
			Status:           engine.StreamStatusStopped,
			TargetRate:       50000,
			PacketsGenerated: 499847,
			PacketsSent:      499847,
			BytesSent:        4498623,
			StartedAt:        startedAt,
			CapturedAt:       startedAt.Add(10 * time.Second),
		},
	})

	for _, want := range []string{
		"[STOPPED: benchmark] sent=499847 bytes=4498623",
		"[SUMMARY: benchmark]  duration=10.000s reason=stopped",
		"Generated: 499847",
		"Sent: 499847",
		"Dropped: 0",
		"Average generation: 49984.7/s | Average sending: 49984.7/s",
		"Target: 50000/s | Latency: 0s | Configured loss: 0.00%",
		"Intentional drops across entire run: 0.00% | Bytes sent: 4498623",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("output = %q, missing %q", output.String(), want)
		}
	}
}

func TestConsoleReporterReportsGenerationAndSendRates(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	reporter := newConsoleReporter(&output)
	snapshot := engine.StreamSnapshot{
		Name:        "telemetry",
		Status:      engine.StreamStatusRunning,
		TargetRate:  1000,
		StartedAt:   startedAt,
		CapturedAt:  startedAt,
		LossPercent: 10,
	}
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateStarted, Snapshot: snapshot})

	snapshot.CapturedAt = startedAt.Add(time.Second)
	snapshot.PacketsGenerated = 1000
	snapshot.PacketsDropped = 100
	snapshot.PacketsSent = 900
	snapshot.BytesSent = 4500
	output.Reset()
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateSnapshot, Snapshot: snapshot})

	want := "1.00s telemetry running 1000 1000.0 900.0 100 10.00 0s"
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if got := strings.Join(strings.Fields(lines[len(lines)-1]), " "); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}

	snapshot.CapturedAt = startedAt.Add(1500 * time.Millisecond)
	snapshot.LossPercent = 0
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateLossChanged, Snapshot: snapshot})

	snapshot.CapturedAt = startedAt.Add(2 * time.Second)
	snapshot.PacketsGenerated = 2000
	snapshot.PacketsSent = 1900
	snapshot.BytesSent = 9500
	output.Reset()
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateSnapshot, Snapshot: snapshot})

	want = "2.00s telemetry running 1000 1000.0 1000.0 100 0.00 0s"
	if got := strings.Join(strings.Fields(output.String()), " "); got != want {
		t.Fatalf("output after disabling loss = %q, want %q", got, want)
	}

	output.Reset()
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateStopped, Snapshot: snapshot})
	for _, want := range []string{
		"Generated: 2000", "Sent: 1900", "Dropped: 100",
		"Configured loss: 0.00%", "Intentional drops across entire run: 5.00%",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("summary = %q, missing %q", output.String(), want)
		}
	}
}

func TestConsoleReporterRatesHandleInvalidIntervals(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		elapsed   time.Duration
		generated uint64
		sent      uint64
	}{
		{name: "same timestamp", generated: 200, sent: 180},
		{name: "earlier timestamp", elapsed: -time.Second, generated: 200, sent: 180},
		{name: "counters reset", elapsed: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			reporter := newConsoleReporter(&output)
			reporter.rememberSample(engine.StreamSnapshot{
				Name:             "telemetry",
				CapturedAt:       startedAt,
				PacketsGenerated: 100,
				PacketsSent:      90,
			})
			generatedRate, sentRate := reporter.actualRates(engine.StreamSnapshot{
				Name:             "telemetry",
				CapturedAt:       startedAt.Add(tt.elapsed),
				PacketsGenerated: tt.generated,
				PacketsSent:      tt.sent,
			})
			if generatedRate != 0 || sentRate != 0 {
				t.Fatalf("rates = (%v, %v), want (0, 0)", generatedRate, sentRate)
			}
		})
	}
}

func TestConsoleReporterSummaryBeforeGeneration(t *testing.T) {
	var output bytes.Buffer
	reporter := newConsoleReporter(&output)
	snapshot := engine.StreamSnapshot{Name: "telemetry"}
	reporter.Report(engine.StreamUpdate{Kind: engine.UpdateCanceled, Snapshot: snapshot})

	for _, want := range []string{
		"[CANCELED: telemetry] sent=0 bytes=0",
		"duration=0.000s reason=canceled",
		"Average generation: 0.0/s | Average sending: 0.0/s",
		"Intentional drops across entire run: 0.00%",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("output = %q, missing %q", output.String(), want)
		}
	}
}
