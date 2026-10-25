package engine

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func TestRunStreamStopsOnContextCancel(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// --- Define test stream
	stream := scenario.Stream{
		Name:     "udp",
		Protocol: scenario.ProtocolUDP,
		Target:   listener.LocalAddr().String(),
		Rate:     10,
		Payload:  "sup",
	}

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan scenario.Event)
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, events, nil, nil, nil)
	}()

	buffer := make([]byte, 1024)
	if _, _, err := listener.ReadFromUDP(buffer); err != nil {
		t.Fatal(err)
	}

	// --- Cancel the context end the RunStream
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after context cancellation")
	}
}

func TestRunStreamPausesChangesRateAndResumes(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	stream := scenario.Stream{
		Name:     "telemetry",
		Protocol: scenario.ProtocolUDP,
		Target:   listener.LocalAddr().String(),
		Rate:     2,
		Payload:  "telemetry",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan scenario.Event, 4)
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, events, nil, nil, nil)
	}()

	waitForUDPPacket(t, listener, 2*time.Second)

	events <- scenario.Event{Action: scenario.ActionPause}
	drainUDPPackets(t, listener, 100*time.Millisecond)
	expectNoUDPPacket(t, listener, 550*time.Millisecond)

	events <- scenario.Event{
		Action: scenario.ActionSetRate,
		Rate:   20,
	}
	expectNoUDPPacket(t, listener, 150*time.Millisecond)

	events <- scenario.Event{Action: scenario.ActionResume}
	waitForUDPPacket(t, listener, 250*time.Millisecond)

	events <- scenario.Event{Action: scenario.ActionStop}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after stop event")
	}
}

func TestRunStreamDelaysPackets(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	stream := scenario.Stream{
		Name:     "delayed",
		Protocol: scenario.ProtocolUDP,
		Target:   listener.LocalAddr().String(),
		Rate:     20,
		Payload:  "delayed",
	}

	events := make(chan scenario.Event, 2)
	events <- scenario.Event{
		Action:  scenario.ActionSetLatency,
		Latency: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, events, nil, nil, nil)
	}()

	expectNoUDPPacket(t, listener, 150*time.Millisecond)
	waitForUDPPacket(t, listener, 300*time.Millisecond)

	events <- scenario.Event{Action: scenario.ActionStop}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after stop event")
	}
}

func TestRunStreamDropsAllPackets(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	stream := scenario.Stream{
		Name:     "loss",
		Protocol: scenario.ProtocolUDP,
		Target:   listener.LocalAddr().String(),
		Rate:     1,
		Payload:  "dropped",
		Seed:     42,
	}

	reporter := &channelReporter{
		updates: make(chan StreamUpdate, 8),
	}
	events := make(chan scenario.Event, 3)
	events <- scenario.Event{
		Action:      scenario.ActionSetLoss,
		LossPercent: 100,
	}
	events <- scenario.Event{
		Action: scenario.ActionSetRate,
		Rate:   1_000,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, events, reporter, nil, nil)
	}()

	waitForUpdate(t, reporter.updates, UpdateStarted)
	lossChanged := waitForUpdate(t, reporter.updates, UpdateLossChanged)
	if lossChanged.Snapshot.LossPercent != 100 {
		t.Fatalf(
			"loss percent = %.2f, want 100",
			lossChanged.Snapshot.LossPercent,
		)
	}
	waitForUpdate(t, reporter.updates, UpdateRateChanged)

	time.Sleep(50 * time.Millisecond)
	events <- scenario.Event{Action: scenario.ActionStop}
	stopped := waitForUpdate(t, reporter.updates, UpdateStopped)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after stop event")
	}

	snapshot := stopped.Snapshot
	if snapshot.PacketsGenerated == 0 {
		t.Fatal("generated zero packets")
	}
	if snapshot.PacketsDropped != snapshot.PacketsGenerated {
		t.Fatalf(
			"dropped packets = %d, want %d generated packets",
			snapshot.PacketsDropped,
			snapshot.PacketsGenerated,
		)
	}
	if snapshot.PacketsSent != 0 {
		t.Fatalf("sent packets = %d, want 0", snapshot.PacketsSent)
	}
	if snapshot.BytesSent != 0 {
		t.Fatalf("sent bytes = %d, want 0", snapshot.BytesSent)
	}

	if err := listener.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1024)
	if _, _, err := listener.ReadFromUDP(buffer); err == nil {
		t.Fatal("received a packet configured for 100% loss")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read UDP packet: %v", err)
	}
}

func TestLatencyPerformanceSmoke(t *testing.T) {
	if os.Getenv("TRAFFICCONTROL_PERF") != "1" {
		t.Skip("set TRAFFICCONTROL_PERF=1 to run the latency performance smoke test")
	}

	const (
		targetRate     = 100_000
		initialRate    = 1
		activeDuration = 2 * time.Second
		latency        = 500 * time.Millisecond
		drainMargin    = 250 * time.Millisecond
	)

	tests := []struct {
		name   string
		jitter time.Duration
	}{
		{name: "baseline"},
		{name: "jitter", jitter: 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}

			listener, err := net.ListenUDP("udp", addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = listener.Close()
			})

			var received atomic.Uint64
			readerDone := make(chan error, 1)
			go func() {
				buffer := make([]byte, 1024)
				for {
					if _, _, err := listener.ReadFromUDP(buffer); err != nil {
						if errors.Is(err, net.ErrClosed) {
							readerDone <- nil
							return
						}

						readerDone <- err
						return
					}

					received.Add(1)
				}
			}()

			stream := scenario.Stream{
				Name:     "latency-performance",
				Protocol: scenario.ProtocolUDP,
				Target:   listener.LocalAddr().String(),
				Rate:     initialRate,
				Payload:  "benchmark",
				Seed:     42,
			}

			reporter := &channelReporter{
				updates: make(chan StreamUpdate, 16),
			}
			events := make(chan scenario.Event, 4)
			events <- scenario.Event{
				Action:  scenario.ActionSetLatency,
				Latency: latency,
			}
			if tt.jitter > 0 {
				events <- scenario.Event{
					Action: scenario.ActionSetJitter,
					Jitter: tt.jitter,
				}
			}
			events <- scenario.Event{
				Action: scenario.ActionSetRate,
				Rate:   targetRate,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- RunStream(ctx, stream, events, reporter, nil, nil)
			}()

			waitForUpdate(t, reporter.updates, UpdateStarted)
			waitForUpdate(t, reporter.updates, UpdateLatencyChanged)
			if tt.jitter > 0 {
				waitForUpdate(t, reporter.updates, UpdateJitterChanged)
			}
			rateChanged := waitForUpdate(t, reporter.updates, UpdateRateChanged)

			time.Sleep(activeDuration)
			events <- scenario.Event{Action: scenario.ActionPause}
			paused := waitForUpdate(t, reporter.updates, UpdatePaused)

			// Pause generation first so queued packets can drain before stop discards them.
			time.Sleep(latency + tt.jitter + drainMargin)
			events <- scenario.Event{Action: scenario.ActionStop}
			stopped := waitForUpdate(t, reporter.updates, UpdateStopped)

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("RunStream returned error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("RunStream did not stop after stop event")
			}

			sent := stopped.Snapshot.PacketsSent
			receiveDeadline := time.Now().Add(time.Second)
			for received.Load() < sent && time.Now().Before(receiveDeadline) {
				time.Sleep(time.Millisecond)
			}

			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-readerDone; err != nil {
				t.Fatalf("read UDP packet: %v", err)
			}

			actualDuration := paused.Snapshot.CapturedAt.Sub(
				rateChanged.Snapshot.CapturedAt,
			)
			expected := uint64(math.Round(float64(targetRate) * actualDuration.Seconds()))
			generated := stopped.Snapshot.PacketsGenerated
			receivedPackets := received.Load()
			if expected == 0 || generated == 0 || sent == 0 {
				t.Fatalf("expected nonzero traffic: expected=%d generated=%d sent=%d", expected, generated, sent)
			}
			pacingError := math.Abs(float64(generated)-float64(expected)) / float64(expected) * 100
			generatedRate := float64(generated) / actualDuration.Seconds()
			delivery := float64(receivedPackets) / float64(sent) * 100

			t.Logf(
				"target=%d packets/s latency=%s jitter=%s active_duration=%s expected=%d generated=%d generated_rate=%.1f packets/s sent=%d received=%d pacing_error=%.4f%% delivery=%.4f%%",
				targetRate,
				latency,
				tt.jitter,
				actualDuration.Round(time.Millisecond),
				expected,
				generated,
				generatedRate,
				sent,
				receivedPackets,
				pacingError,
				delivery,
			)

			if pacingError > 2 {
				t.Errorf("generation pacing error %.4f%% exceeds 2%%", pacingError)
			}
			if dropped := stopped.Snapshot.PacketsDropped; dropped != 0 {
				t.Errorf("unexpected intentional drops: %d", dropped)
			}
			if sent != generated {
				t.Errorf("queue did not fully drain: generated=%d sent=%d", generated, sent)
			}
			if delivery < 99.99 {
				t.Errorf("delivery %.4f%% is below 99.99%%", delivery)
			}
		})
	}
}

func waitForUDPPacket(t *testing.T, listener *net.UDPConn, timeout time.Duration) {
	t.Helper()

	if err := listener.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1024)
	if _, _, err := listener.ReadFromUDP(buffer); err != nil {
		t.Fatalf("timed out waiting for UDP packet: %v", err)
	}
}

func drainUDPPackets(t *testing.T, listener *net.UDPConn, duration time.Duration) {
	t.Helper()

	if err := listener.SetReadDeadline(time.Now().Add(duration)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1024)
	for {
		if _, _, err := listener.ReadFromUDP(buffer); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			t.Fatalf("drain UDP packets: %v", err)
		}
	}
}

func expectNoUDPPacket(t *testing.T, listener *net.UDPConn, duration time.Duration) {
	t.Helper()

	if err := listener.SetReadDeadline(time.Now().Add(duration)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 1024)
	if _, _, err := listener.ReadFromUDP(buffer); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return
		}
		t.Fatalf("read UDP packet: %v", err)
	}

	t.Fatal("received an unexpected UDP packet while stream was paused")
}
