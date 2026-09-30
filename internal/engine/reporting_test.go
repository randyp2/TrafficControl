package engine

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

type channelReporter struct {
	updates chan StreamUpdate
}

func (r *channelReporter) Report(update StreamUpdate) {
	r.updates <- update
}

func waitForUpdate(
	t *testing.T,
	updates <-chan StreamUpdate,
	want UpdateKind,
) StreamUpdate {
	t.Helper()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		select {
		case update := <-updates:
			if update.Kind == want {
				return update
			}

			if update.Kind == UpdateSnapshot {
				continue
			}

			t.Fatalf(
				"received update %q, want %q",
				update.Kind,
				want,
			)

		case <-timer.C:
			t.Fatalf("timed out waiting for update %q", want)
		}
	}
}

func TestRunStreamReportsStarted(t *testing.T) {
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
		Rate:     1,
		Payload:  "hello",
	}

	reporter := &channelReporter{
		updates: make(chan StreamUpdate, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunStream(
			ctx,
			stream,
			make(chan scenario.Event),
			reporter,
		)
	}()

	select {
	case update := <-reporter.updates:
		if update.Kind != UpdateStarted {
			t.Fatalf(
				"update kind = %q, want %q",
				update.Kind,
				UpdateStarted,
			)
		}

		snapshot := update.Snapshot
		if snapshot.Name != stream.Name {
			t.Fatalf(
				"stream name = %q, want %q",
				snapshot.Name,
				stream.Name,
			)
		}

		if snapshot.Status != StreamStatusRunning {
			t.Fatalf(
				"status = %q, want %q",
				snapshot.Status,
				StreamStatusRunning,
			)
		}

		if snapshot.TargetRate != stream.Rate {
			t.Fatalf(
				"target rate = %d, want %d",
				snapshot.TargetRate,
				stream.Rate,
			)
		}

		if snapshot.PacketsSent != 0 {
			t.Fatalf(
				"packets sent = %d, want 0",
				snapshot.PacketsSent,
			)
		}

		if snapshot.BytesSent != 0 {
			t.Fatalf(
				"bytes sent = %d, want 0",
				snapshot.BytesSent,
			)
		}

		if snapshot.StartedAt.IsZero() {
			t.Fatal("started time is zero")
		}

		if snapshot.UpdatedAt.IsZero() {
			t.Fatal("updated time is zero")
		}

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started update")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after cancellation")
	}
}

func TestRunStreamReportsSnapshotCounters(t *testing.T) {
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
		Rate:     20,
		Payload:  "bruh",
	}

	reporter := &channelReporter{
		updates: make(chan StreamUpdate, 2),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, make(chan scenario.Event), reporter)
	}()

	select {
	case update := <-reporter.updates:
		if update.Kind != UpdateStarted {
			t.Fatalf(
				"first update = %q, want %q",
				update.Kind,
				UpdateStarted,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started update")
	}

	select {
	case update := <-reporter.updates:
		if update.Kind != UpdateSnapshot {
			t.Fatalf(
				"update = %q, want %q",
				update.Kind,
				UpdateSnapshot,
			)
		}

		snapshot := update.Snapshot
		if snapshot.PacketsSent == 0 {
			t.Fatal("snapshot reported zero packets")
		}

		wantBytes := snapshot.PacketsSent * uint64(len(stream.Payload))
		if snapshot.BytesSent != wantBytes {
			t.Fatalf(
				"bytes sent = %d, want %d",
				snapshot.BytesSent,
				wantBytes,
			)
		}

		if !snapshot.UpdatedAt.After(snapshot.StartedAt) {
			t.Fatal("snapshot update time was not advanced")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for snapshot update")
	}

	select {
	case err := <-done:
		t.Fatalf("RunStream returned after snapshot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after cancellation")
	}
}

func TestRunStreamReportsEventTransitions(t *testing.T) {
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
		Rate:     10,
		Payload:  "hello",
	}

	reporter := &channelReporter{
		updates: make(chan StreamUpdate, 16),
	}
	events := make(chan scenario.Event, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunStream(ctx, stream, events, reporter)
	}()

	started := waitForUpdate(t, reporter.updates, UpdateStarted)
	if started.Snapshot.Status != StreamStatusRunning {
		t.Fatalf(
			"started status = %q, want %q",
			started.Snapshot.Status,
			StreamStatusRunning,
		)
	}

	events <- scenario.Event{Action: scenario.ActionPause}

	paused := waitForUpdate(t, reporter.updates, UpdatePaused)
	if paused.Snapshot.Status != StreamStatusPaused {
		t.Fatalf(
			"paused status = %q, want %q",
			paused.Snapshot.Status,
			StreamStatusPaused,
		)
	}

	events <- scenario.Event{
		Action: scenario.ActionSetRate,
		Rate:   25,
	}

	rateChanged := waitForUpdate(t, reporter.updates, UpdateRateChanged)
	if rateChanged.Snapshot.TargetRate != 25 {
		t.Fatalf(
			"target rate = %d, want 25",
			rateChanged.Snapshot.TargetRate,
		)
	}
	if rateChanged.Snapshot.Status != StreamStatusPaused {
		t.Fatalf(
			"rate-change status = %q, want %q",
			rateChanged.Snapshot.Status,
			StreamStatusPaused,
		)
	}

	events <- scenario.Event{Action: scenario.ActionResume}

	resumed := waitForUpdate(t, reporter.updates, UpdateResumed)
	if resumed.Snapshot.Status != StreamStatusRunning {
		t.Fatalf(
			"resumed status = %q, want %q",
			resumed.Snapshot.Status,
			StreamStatusRunning,
		)
	}
	if resumed.Snapshot.TargetRate != 25 {
		t.Fatalf(
			"resumed target rate = %d, want 25",
			resumed.Snapshot.TargetRate,
		)
	}

	events <- scenario.Event{Action: scenario.ActionStop}

	stopped := waitForUpdate(t, reporter.updates, UpdateStopped)
	if stopped.Snapshot.Status != StreamStatusStopped {
		t.Fatalf(
			"stopped status = %q, want %q",
			stopped.Snapshot.Status,
			StreamStatusStopped,
		)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunStream returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunStream did not stop after stop event")
	}
}
