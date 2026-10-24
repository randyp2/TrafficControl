package engine

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/receiver"
	"github.com/randyp2/trafficcontrol/internal/scenario"
)

type tcpReceiverReporter struct {
	snapshots chan receiver.Snapshot
}

func (r *tcpReceiverReporter) Report(snapshot receiver.Snapshot) {
	r.snapshots <- snapshot
}

func TestRunTCPToReceiver(t *testing.T) {
	r, err := receiver.ListenTCP("127.0.0.1:0", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics := &tcpReceiverReporter{snapshots: make(chan receiver.Snapshot, 128)}
	receiverDone := make(chan error, 1)
	go func() { receiverDone <- r.Run(ctx, metrics) }()

	s := scenario.Scenario{
		Name: "tcp-e2e",
		Streams: []scenario.Stream{
			{Name: "first", Protocol: scenario.ProtocolTCP, Target: r.Address(), Rate: 1000, Payload: "first"},
			{Name: "second", Protocol: scenario.ProtocolTCP, Target: r.Address(), Rate: 1000, Payload: "second"},
		},
		Events: []scenario.Event{
			{At: 20 * time.Millisecond, Stream: "first", Action: scenario.ActionSetLatency, Latency: 10 * time.Millisecond},
			{At: 40 * time.Millisecond, Stream: "first", Action: scenario.ActionSetLoss, LossPercent: 100},
			{At: 60 * time.Millisecond, Stream: "first", Action: scenario.ActionSetLoss, LossPercent: 0},
			{At: 80 * time.Millisecond, Stream: "first", Action: scenario.ActionSetLatency, Latency: 0},
			{At: 120 * time.Millisecond, Stream: "first", Action: scenario.ActionStop},
			{At: 150 * time.Millisecond, Stream: "second", Action: scenario.ActionStop},
		},
	}
	reporter := &channelReporter{updates: make(chan StreamUpdate, 32)}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, s, reporter, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TCP scenario did not stop")
	}

	var sentBytes uint64
	stopped := 0
	for len(reporter.updates) > 0 {
		update := <-reporter.updates
		if update.Kind == UpdateFailed || update.Kind == UpdateCanceled {
			t.Fatalf("unexpected terminal update: %+v", update)
		}
		if update.Kind == UpdateStopped {
			stopped++
			sentBytes += update.Snapshot.BytesSent
			if update.Snapshot.PacketsSent == 0 {
				t.Fatalf("stream %q sent nothing", update.Snapshot.Name)
			}
		}
	}
	if stopped != 2 {
		t.Fatalf("stopped streams = %d, want 2", stopped)
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case snapshot := <-metrics.snapshots:
			if snapshot.BytesReceived == sentBytes && snapshot.ConnectionsAccepted == 2 && snapshot.ActiveConnections == 0 {
				cancel()
				select {
				case err := <-receiverDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("TCP receiver did not stop")
				}
				t.Logf("two TCP streams sent and received %d bytes", sentBytes)
				return
			}
		case <-timer.C:
			t.Fatalf("receiver did not observe %d bytes across two completed connections", sentBytes)
		}
	}
}

func TestRunTCPCancellationInterruptsBlockedWrite(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scheduled bool
		latency   time.Duration
	}{
		{name: "context cancellation"},
		{name: "delayed write cancellation", latency: 10 * time.Millisecond},
		{name: "scheduled stop", scheduled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reporter := &channelReporter{updates: make(chan StreamUpdate, 16)}
			stream := scenario.Stream{
				Name: "blocked", Protocol: scenario.ProtocolTCP, Target: listener.Addr().String(),
				Rate: 1000, Payload: strings.Repeat("x", 8*1024*1024),
			}
			events := make(chan scenario.Event, 1)
			if tt.latency > 0 {
				events <- scenario.Event{Action: scenario.ActionSetLatency, Latency: tt.latency}
			}
			done := make(chan error, 1)
			go func() {
				if tt.scheduled {
					done <- Run(ctx, scenario.Scenario{
						Name: "blocked-stop", Streams: []scenario.Stream{stream},
						Events: []scenario.Event{{At: 250 * time.Millisecond, Stream: stream.Name, Action: scenario.ActionStop}},
					}, reporter, nil)
				} else {
					done <- RunStream(ctx, stream, events, reporter)
				}
			}()
			peer, err := listener.AcceptTCP()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			if err := peer.SetReadBuffer(1024); err != nil {
				t.Fatal(err)
			}
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Read(make([]byte, 1)); err != nil {
				t.Fatalf("wait for TCP write to begin: %v", err)
			}
			select {
			case err := <-done:
				t.Fatalf("stream returned before cancellation: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			started := time.Now()
			if !tt.scheduled {
				cancel()
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("cancellation returned a send failure: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation waited for the two-second write timeout")
			}
			want := UpdateCanceled
			if tt.scheduled {
				want = UpdateStopped
			}
			var last StreamUpdate
			for len(reporter.updates) > 0 {
				last = <-reporter.updates
			}
			if last.Kind != want || last.Snapshot.LastError != "" {
				t.Fatalf("terminal update = %+v, want %q without an error", last, want)
			}
			t.Logf("blocked write ended in %s with %s", time.Since(started).Round(time.Millisecond), last.Kind)
		})
	}
}

func TestRunTCPWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reporter := &channelReporter{updates: make(chan StreamUpdate, 2)}
	if err := RunStream(ctx, scenario.Stream{
		Name: "canceled", Protocol: scenario.ProtocolTCP, Target: "127.0.0.1:6000", Rate: 1,
	}, nil, reporter); err != nil {
		t.Fatal(err)
	}
	if update := <-reporter.updates; update.Kind != UpdateCanceled {
		t.Fatalf("update = %q, want canceled", update.Kind)
	}
}
