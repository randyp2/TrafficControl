package engine

import (
	"context"
	"net"
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
		done <- RunStream(ctx, stream, events, nil)
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

func TestRunMultipleStreams(t *testing.T) {
	addr1, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	addr2, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener1, err := net.ListenUDP("udp", addr1)
	if err != nil {
		t.Fatal(err)
	}

	listener2, err := net.ListenUDP("udp", addr2)
	if err != nil {
		t.Fatal(err)
	}

	defer listener1.Close()
	defer listener2.Close()

	s := scenario.Scenario{
		Name: "multi-stream-test",
		Streams: []scenario.Stream{
			{
				Name:     "stream-one",
				Protocol: scenario.ProtocolUDP,
				Target:   listener1.LocalAddr().String(),
				Rate:     10,
				Payload:  "one",
			},
			{
				Name:     "stream-two",
				Protocol: scenario.ProtocolUDP,
				Target:   listener2.LocalAddr().String(),
				Rate:     10,
				Payload:  "two",
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- Run(ctx, s, nil)
	}()

	// --- Test listener1
	buffer1 := make([]byte, 1024)
	n1, _, err := listener1.ReadFromUDP(buffer1)
	if err != nil {
		t.Fatal(err)
	}

	if got := string(buffer1[:n1]); got != "one" {
		t.Fatalf("listener1 got %q, want %q", got, "one")
	}

	// --- Test listener2
	buffer2 := make([]byte, 1024)
	n2, _, err := listener2.ReadFromUDP(buffer2)
	if err != nil {
		t.Fatal(err)
	}

	if got := string(buffer2[:n2]); got != "two" {
		t.Fatalf("listener2 got %q, want %q", got, "two")
	}

	// --- Test cancellation
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}

	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
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
		done <- RunStream(ctx, stream, events, nil)
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
