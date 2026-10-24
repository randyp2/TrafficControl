package receiver

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTCPReceiverReportsMultipleConnectionsAndClosesIdlePeers(t *testing.T) {
	r, err := ListenTCP("127.0.0.1:0", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &channelReporter{snapshots: make(chan Snapshot, 128)}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, reporter) }()

	first, err := net.DialTimeout("tcp", r.Address(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := net.DialTimeout("tcp", r.Address(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, payload := range []string{"one", "two"} {
		if _, err := first.Write([]byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := second.Write([]byte("three")); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForSnapshot(t, reporter.snapshots, func(s Snapshot) bool {
		return s.BytesReceived == 11 && s.ConnectionsAccepted == 2 && s.ActiveConnections == 2
	})
	if snapshot.Protocol != ProtocolTCP || snapshot.DatagramsReceived != 0 {
		t.Fatalf("TCP snapshot has incorrect protocol or datagram count: %+v", snapshot)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSnapshot(t, reporter.snapshots, func(s Snapshot) bool { return s.ActiveConnections == 1 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not stop with an idle peer connected")
	}
	final := waitForSnapshot(t, reporter.snapshots, func(s Snapshot) bool { return s.Final })
	if final.BytesReceived != 11 || final.ConnectionsAccepted != 2 || final.ActiveConnections != 0 || final.ConnectionErrors != 0 {
		t.Fatalf("incorrect final TCP totals: %+v", final)
	}
	if err := second.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection was not closed")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("idle connection remained open after receiver cancellation")
	}
}

func TestTCPReceiverStopsWithoutConnections(t *testing.T) {
	r, err := ListenTCP("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver stayed blocked in Accept")
	}
}

func TestTCPReceiverReturnsAcceptError(t *testing.T) {
	r, err := ListenTCP("127.0.0.1:0", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "accept TCP connection") {
		t.Fatalf("Run error = %v, want an accept error", err)
	}
}

func TestListenTCPRequiresAddress(t *testing.T) {
	r, err := ListenTCP("", time.Second)
	if err == nil || r != nil {
		t.Fatalf("ListenTCP returned (%v, %v), want an address error", r, err)
	}
}

func TestTCPReceiverReportsConnectionReset(t *testing.T) {
	r, err := ListenTCP("127.0.0.1:0", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &channelReporter{snapshots: make(chan Snapshot, 128)}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, reporter) }()
	conn, err := net.DialTimeout("tcp", r.Address(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitForSnapshot(t, reporter.snapshots, func(s Snapshot) bool { return s.ActiveConnections == 1 })
	if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSnapshot(t, reporter.snapshots, func(s Snapshot) bool {
		return s.ConnectionErrors == 1 && s.ActiveConnections == 0
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not stop after a connection reset")
	}
}
