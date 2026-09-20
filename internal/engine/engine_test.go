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
	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream)
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
		done <- Run(ctx, s)
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
