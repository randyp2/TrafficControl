package engine

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func TestSampleDelayBounds(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		name    string
		latency time.Duration
		jitter  time.Duration
	}{
		{name: "fixed latency", latency: 100 * time.Millisecond},
		{name: "normal jitter", latency: 100 * time.Millisecond, jitter: 10 * time.Millisecond},
		{name: "jitter equals latency", latency: 10 * time.Nanosecond, jitter: 10 * time.Nanosecond},
		{name: "maximum fixed latency", latency: maxDuration},
		{name: "maximum combined delay", latency: maxDuration/2 + 1, jitter: maxDuration / 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(42))
			for range 1000 {
				got := sampleDelay(rng, tt.latency, tt.jitter)
				if got < tt.latency-tt.jitter || got > tt.latency+tt.jitter {
					t.Fatalf("delay %s outside [%s, %s]", got, tt.latency-tt.jitter, tt.latency+tt.jitter)
				}
			}
		})
	}
}

func TestSampleDelayIncludesEndpoints(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	seen := make(map[time.Duration]bool)
	for range 1000 {
		seen[sampleDelay(rng, 2*time.Nanosecond, time.Nanosecond)] = true
	}
	if !seen[time.Nanosecond] || !seen[3*time.Nanosecond] {
		t.Fatalf("did not include both jitter endpoints: %v", seen)
	}
}

func TestSampleDelayReproducible(t *testing.T) {
	first := rand.New(rand.NewSource(42))
	second := rand.New(rand.NewSource(42))
	if got := sampleDelay(first, 100*time.Millisecond, 0); got != 100*time.Millisecond {
		t.Fatalf("zero jitter delay = %s, want 100ms", got)
	}
	for range 100 {
		a := sampleDelay(first, 100*time.Millisecond, 10*time.Millisecond)
		b := sampleDelay(second, 100*time.Millisecond, 10*time.Millisecond)
		if a != b {
			t.Fatalf("same seed produced different delays: %s and %s", a, b)
		}
	}
}

func TestHandleEventRejectsJitterOverflow(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	for _, tt := range []struct {
		name        string
		latency     time.Duration
		jitter      time.Duration
		event       scenario.Event
		wantLatency time.Duration
		wantJitter  time.Duration
		wantErr     bool
	}{
		{name: "set jitter overflow", latency: maxDuration,
			event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: 1}, wantLatency: maxDuration, wantErr: true},
		{name: "set latency overflow", latency: 2, jitter: 1,
			event: scenario.Event{Action: scenario.ActionSetLatency, Latency: maxDuration}, wantLatency: 2, wantJitter: 1, wantErr: true},
		{name: "set jitter at limit", latency: maxDuration - 1,
			event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: 1}, wantLatency: maxDuration - 1, wantJitter: 1},
		{name: "set latency at limit", latency: 2, jitter: 1,
			event: scenario.Event{Action: scenario.ActionSetLatency, Latency: maxDuration - 1}, wantLatency: maxDuration - 1, wantJitter: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pacer := newTestPacer(t, 10, time.Now())
			state := streamState{targetRate: 10, status: streamRunning, latency: tt.latency, jitter: tt.jitter}
			want := state
			want.latency, want.jitter = tt.wantLatency, tt.wantJitter
			stop, err := handleEvent(pacer, tt.event, &state)
			if (err != nil) != tt.wantErr || stop || state != want {
				t.Fatalf("handleEvent = (%t, %v), state=%+v; wantErr=%t state=%+v", stop, err, state, tt.wantErr, want)
			}
		})
	}
}

func TestHandleEventJitter(t *testing.T) {
	const latency = 100 * time.Millisecond
	const jitter = 10 * time.Millisecond
	for _, tt := range []struct {
		name        string
		initial     time.Duration
		status      streamStatus
		event       scenario.Event
		wantJitter  time.Duration
		wantLatency time.Duration
		wantErr     bool
	}{
		{name: "enable", event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: jitter}, wantJitter: jitter, wantLatency: latency},
		{name: "enable while paused", status: streamPaused, event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: jitter}, wantJitter: jitter, wantLatency: latency},
		{name: "equal to latency", event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: latency}, wantJitter: latency, wantLatency: latency},
		{name: "disable", initial: jitter, event: scenario.Event{Action: scenario.ActionSetJitter}, wantLatency: latency},
		{name: "negative rejected", initial: jitter, event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: -1}, wantJitter: jitter, wantLatency: latency, wantErr: true},
		{name: "too large rejected", initial: jitter, event: scenario.Event{Action: scenario.ActionSetJitter, Jitter: latency + 1}, wantJitter: jitter, wantLatency: latency, wantErr: true},
		{name: "latency below active jitter rejected", initial: jitter, event: scenario.Event{Action: scenario.ActionSetLatency, Latency: jitter - 1}, wantJitter: jitter, wantLatency: latency, wantErr: true},
		{name: "latency equal to jitter", initial: jitter, event: scenario.Event{Action: scenario.ActionSetLatency, Latency: jitter}, wantJitter: jitter, wantLatency: jitter},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pacer := newTestPacer(t, 10, time.Now())
			if tt.status == streamPaused {
				pacer.Pause()
			}
			state := streamState{targetRate: 10, status: tt.status, latency: latency, jitter: tt.initial}
			want := state
			want.latency = tt.wantLatency
			want.jitter = tt.wantJitter
			stop, err := handleEvent(pacer, tt.event, &state)
			if (err != nil) != tt.wantErr || stop {
				t.Fatalf("handleEvent = (%t, %v), want stop=false wantErr=%t", stop, err, tt.wantErr)
			}
			if state != want {
				t.Fatalf("state = %+v, want %+v", state, want)
			}
			if pacer.rate != 10 || pacer.running != (tt.status == streamRunning) {
				t.Fatal("jitter event changed pacing or pause state")
			}
		})
	}
}

func TestRunStreamReportsJitterChanges(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &channelReporter{updates: make(chan StreamUpdate, 8)}
	events := make(chan scenario.Event, 4)
	events <- scenario.Event{Action: scenario.ActionSetLatency, Latency: 100 * time.Millisecond}
	events <- scenario.Event{Action: scenario.ActionSetJitter, Jitter: 10 * time.Millisecond}
	events <- scenario.Event{Action: scenario.ActionSetJitter}
	events <- scenario.Event{Action: scenario.ActionStop}
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, scenario.Stream{
			Name: "telemetry", Protocol: scenario.ProtocolUDP,
			Target: listener.LocalAddr().String(), Rate: 1, Payload: "jitter",
		}, events, reporter, nil, nil)
	}()
	waitForUpdate(t, reporter.updates, UpdateStarted)
	waitForUpdate(t, reporter.updates, UpdateLatencyChanged)
	for _, want := range []time.Duration{10 * time.Millisecond, 0} {
		update := waitForUpdate(t, reporter.updates, UpdateJitterChanged)
		if update.Snapshot.Jitter != want || update.Snapshot.Latency != 100*time.Millisecond {
			t.Fatalf("jitter snapshot = %+v, want jitter %s with latency 100ms", update.Snapshot, want)
		}
		if update.Snapshot.UpdatedAt.Before(update.Snapshot.StartedAt) {
			t.Fatal("jitter update time precedes stream start")
		}
	}
	waitForUpdate(t, reporter.updates, UpdateStopped)
	waitForReadinessResult(t, done)
}
