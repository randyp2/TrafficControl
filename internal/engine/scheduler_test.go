package engine

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func TestScheduleEventsRoutesToCorrectStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	telemetry := make(chan scenario.Event, 1)
	backend := make(chan scenario.Event, 1)

	channels := map[string]chan scenario.Event{
		"telemetry": telemetry,
		"backend":   backend,
	}

	want := scenario.Event{
		Stream: "telemetry",
		Action: scenario.ActionStop,
	}

	done := make(chan error, 1)
	go func() {
		done <- scheduleEvents(
			ctx,
			time.Now(),
			[]scenario.Event{want},
			channels,
		)
	}()

	select {
	case got := <-telemetry:
		if got != want {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for telemetry event")
	}

	select {
	case got := <-backend:
		t.Fatalf("backend unexpectedly received event %#v", got)
	default:
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("scheduleEvents returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduleEvents did not return")
	}
}

func TestRunDeliversScheduledStop(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Keep these names different so event channels must be keyed by stream name.
	s := scenario.Scenario{
		Name: "scheduler-test",
		Streams: []scenario.Stream{
			{
				Name:     "telemetry",
				Protocol: scenario.ProtocolUDP,
				Target:   listener.LocalAddr().String(),
				Rate:     1,
				Payload:  "hello",
			},
		},
		Events: []scenario.Event{
			{
				Stream: "telemetry",
				Action: scenario.ActionStop,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, s)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after the scheduled stop event")
	}
}
