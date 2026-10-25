package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func TestRunStreamWaitsForSharedStart(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	starts := make(chan time.Time, 1)
	events := make(chan scenario.Event, 1)
	reporter := &channelReporter{updates: make(chan StreamUpdate, 8)}
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, scenario.Stream{
			Name: "waiting", Protocol: scenario.ProtocolUDP,
			Target: listener.LocalAddr().String(), Rate: 1000, Payload: "ready",
		}, events, reporter, ready, starts)
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("stream exited before becoming ready: %v", err)
	case <-time.After(time.Second):
		t.Fatal("stream did not announce readiness")
	}

	if err := listener.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	if _, _, err := listener.ReadFrom(buffer); err == nil {
		t.Fatal("stream sent traffic before receiving the shared start")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before release: %v", err)
	}
	select {
	case update := <-reporter.updates:
		t.Fatalf("stream reported %q before release", update.Kind)
	default:
	}

	start := time.Now()
	starts <- start
	started := waitForUpdate(t, reporter.updates, UpdateStarted)
	if !started.Snapshot.StartedAt.Equal(start) || !started.Snapshot.CapturedAt.Equal(start) {
		t.Fatalf("stream did not use the shared start: %+v", started.Snapshot)
	}
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := listener.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("stream did not send after release: %v", err)
	}
	if got := string(buffer[:n]); got != "ready" {
		t.Fatalf("payload = %q, want ready", got)
	}
	events <- scenario.Event{Action: scenario.ActionStop}
	waitForReadinessResult(t, done)
	waitForUpdate(t, reporter.updates, UpdateStopped)
}

func TestRunStreamCancelsWhileWaitingForStart(t *testing.T) {
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
	ready := make(chan struct{}, 1)
	starts := make(chan time.Time, 1)
	reporter := &channelReporter{updates: make(chan StreamUpdate, 4)}
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, scenario.Stream{
			Name: "waiting", Protocol: scenario.ProtocolTCP,
			Target: listener.Addr().String(), Rate: 1000, Payload: "ready",
		}, nil, reporter, ready, starts)
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("stream exited before becoming ready: %v", err)
	case <-time.After(time.Second):
		t.Fatal("stream did not announce readiness")
	}
	peer, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	cancel()
	waitForReadinessResult(t, done)
	canceled := waitForUpdate(t, reporter.updates, UpdateCanceled)
	if canceled.Snapshot.PacketsGenerated != 0 || canceled.Snapshot.PacketsSent != 0 {
		t.Fatalf("waiting stream generated traffic: %+v", canceled.Snapshot)
	}
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("peer read = %v, want EOF after closing the waiting stream", err)
	}
}

func TestRunReadinessUsesOneStartForAllStreams(t *testing.T) {
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	const duration = 100 * time.Millisecond
	s := scenario.Scenario{
		Name: "shared-start",
		Streams: []scenario.Stream{
			{Name: "udp", Protocol: scenario.ProtocolUDP, Target: udp.LocalAddr().String(), Rate: 100, Payload: "udp"},
			{Name: "tcp", Protocol: scenario.ProtocolTCP, Target: tcp.Addr().String(), Rate: 100, Payload: "tcp"},
		},
		Events: []scenario.Event{
			{At: duration, Stream: "udp", Action: scenario.ActionStop},
			{At: duration, Stream: "tcp", Action: scenario.ActionStop},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &channelReporter{updates: make(chan StreamUpdate, 16)}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, s, reporter, nil) }()
	waitForReadinessResult(t, done)

	started := make(map[string]time.Time)
	stopped := make(map[string]time.Time)
	for len(reporter.updates) > 0 {
		update := <-reporter.updates
		switch update.Kind {
		case UpdateStarted:
			started[update.Snapshot.Name] = update.Snapshot.StartedAt
		case UpdateStopped:
			stopped[update.Snapshot.Name] = update.Snapshot.CapturedAt
		default:
			t.Fatalf("unexpected readiness update: %+v", update)
		}
	}
	if len(started) != 2 || len(stopped) != 2 {
		t.Fatalf("started %d streams and stopped %d, want two each", len(started), len(stopped))
	}
	if started["udp"].IsZero() || !started["udp"].Equal(started["tcp"]) {
		t.Fatalf("stream starts differ: udp=%s tcp=%s", started["udp"], started["tcp"])
	}
	for name, start := range started {
		if stopped[name].Before(start.Add(duration)) {
			t.Fatalf("stream %q stopped before its full scenario duration", name)
		}
	}
}

func TestRunReadinessFailureCancelsOtherStreams(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s := scenario.Scenario{
		Name: "setup-failure",
		Streams: []scenario.Stream{
			{Name: "waiting", Protocol: scenario.ProtocolUDP, Target: listener.LocalAddr().String(), Rate: 1000, Payload: "ready"},
			{Name: "broken", Protocol: scenario.ProtocolTCP, Target: "127.0.0.1:70000", Rate: 1000, Payload: "broken"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &channelReporter{updates: make(chan StreamUpdate, 8)}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, s, reporter, nil) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "127.0.0.1:70000") {
			t.Fatalf("Run error = %v, want the original TCP setup error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run hung waiting for readiness after a setup failure")
	}
	canceled := waitForUpdate(t, reporter.updates, UpdateCanceled)
	if canceled.Snapshot.Name != "waiting" || canceled.Snapshot.PacketsGenerated != 0 || canceled.Snapshot.PacketsSent != 0 {
		t.Fatalf("unexpected cancellation after setup failure: %+v", canceled)
	}
	if len(reporter.updates) != 0 {
		t.Fatal("scenario emitted extra updates after failing to initialize")
	}
}

func waitForReadinessResult(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not finish")
	}
}
