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
		Name:             "telemetry",
		Status:           engine.StreamStatusRunning,
		TargetRate:       25,
		LossPercent:      50,
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
			want: "[STARTED: telemetry] rate=25 pkt/s\n",
		},
		{
			name: "snapshot",
			kind: engine.UpdateSnapshot,
			want: "[SNAPSHOT: telemetry] status=running target=25 packets/s generated_rate=0.0 packets/s actual=0.0 sends/s sends=10 bytes=50 generated=20 dropped=10 loss_percent=50.00 observed_loss_percent=50.00\n",
		},
		{
			name: "rate changed",
			kind: engine.UpdateRateChanged,
			want: "[RATE_CHANGED: telemetry] rate=25 pkt/s status=running\n",
		},
		{
			name: "loss changed",
			kind: engine.UpdateLossChanged,
			want: "[PACKET_LOSS_CHANGED: telemetry] loss_percent=50.00 status=running\n",
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
				"[SUMMARY: telemetry] reason=stopped duration=2.000s target=25 packets/s packets=10 bytes=50 average=5.0 packets/s generated=20 dropped=10 average_generated=10.0 packets/s loss_percent=50.00 observed_loss_percent=50.00\n",
		},
		{
			name: "canceled",
			kind: engine.UpdateCanceled,
			want: "[CANCELED: telemetry] packets=10 bytes=50\n" +
				"[SUMMARY: telemetry] reason=canceled duration=2.000s target=25 packets/s packets=10 bytes=50 average=5.0 packets/s generated=20 dropped=10 average_generated=10.0 packets/s loss_percent=50.00 observed_loss_percent=50.00\n",
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

	want := "[STOPPED: benchmark] packets=499847 bytes=4498623\n" +
		"[SUMMARY: benchmark] reason=stopped duration=10.000s target=50000 packets/s packets=499847 bytes=4498623 average=49984.7 packets/s generated=499847 dropped=0 average_generated=49984.7 packets/s loss_percent=0.00 observed_loss_percent=0.00\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
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

	want := "[SNAPSHOT: telemetry] status=running target=1000 packets/s generated_rate=1000.0 packets/s actual=900.0 sends/s sends=900 bytes=4500 generated=1000 dropped=100 loss_percent=10.00 observed_loss_percent=10.00\n"
	if got := output.String(); got != want {
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

	want = "[SNAPSHOT: telemetry] status=running target=1000 packets/s generated_rate=1000.0 packets/s actual=1000.0 sends/s sends=1900 bytes=9500 generated=2000 dropped=100 loss_percent=0.00 observed_loss_percent=5.00\n"
	if got := output.String(); got != want {
		t.Fatalf("output after disabling loss = %q, want %q", got, want)
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

	want := "[CANCELED: telemetry] packets=0 bytes=0\n" +
		"[SUMMARY: telemetry] reason=canceled duration=0.000s target=0 packets/s packets=0 bytes=0 average=0.0 packets/s generated=0 dropped=0 average_generated=0.0 packets/s loss_percent=0.00 observed_loss_percent=0.00\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}
