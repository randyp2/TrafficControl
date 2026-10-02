package receiver

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

type channelReporter struct {
	snapshots chan Snapshot
}

func (r *channelReporter) Report(snapshot Snapshot) {
	r.snapshots <- snapshot
}

func TestUDPReceiverReportsTrafficAndFinalSnapshot(t *testing.T) {
	udpReceiver, err := ListenUDP("127.0.0.1:0", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}

	reporter := &channelReporter{
		snapshots: make(chan Snapshot, 32),
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)

	go func() {
		runResult <- udpReceiver.Run(ctx, reporter)
	}()

	conn, err := net.Dial("udp", udpReceiver.Address())
	if err != nil {
		cancel()
		<-runResult
		t.Fatalf("net.Dial() error = %v", err)
	}
	defer conn.Close()

	payloads := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	}

	for _, payload := range payloads {
		if _, err := conn.Write(payload); err != nil {
			cancel()
			<-runResult
			t.Fatalf("conn.Write() error = %v", err)
		}
	}

	waitForSnapshot(t, reporter.snapshots, func(snapshot Snapshot) bool {
		return snapshot.DatagramsReceived == 3 && snapshot.BytesReceived == 11
	})

	cancel()

	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run() to stop")
	}

	final := waitForSnapshot(t, reporter.snapshots, func(snapshot Snapshot) bool {
		return snapshot.Final
	})

	if final.Protocol != ProtocolUDP {
		t.Fatalf("final protocol = %q, want %q", final.Protocol, ProtocolUDP)
	}
	if final.Address != udpReceiver.Address() {
		t.Fatalf("final address = %q, want %q", final.Address, udpReceiver.Address())
	}
	if final.DatagramsReceived != 3 {
		t.Fatalf("final datagrams = %d, want 3", final.DatagramsReceived)
	}
	if final.BytesReceived != 11 {
		t.Fatalf("final bytes = %d, want 11", final.BytesReceived)
	}
}

func TestUDPReceiverReturnsReadError(t *testing.T) {
	udpReceiver, err := ListenUDP("127.0.0.1:0", time.Second)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}

	if err := udpReceiver.conn.Close(); err != nil {
		t.Fatalf("conn.Close() error = %v", err)
	}

	err = udpReceiver.Run(context.Background(), nil)
	if err == nil {
		t.Fatal("Run() error = nil, want a read error")
	}
	if !strings.Contains(err.Error(), "read UDP traffic") {
		t.Fatalf("Run() error = %q", err)
	}
}

func TestListenUDPRequiresAddress(t *testing.T) {
	udpReceiver, err := ListenUDP("", time.Second)
	if err == nil {
		t.Fatal("ListenUDP() error = nil, want an address error")
	}
	if udpReceiver != nil {
		t.Fatalf("ListenUDP() receiver = %v, want nil", udpReceiver)
	}
}

func waitForSnapshot(
	t *testing.T,
	snapshots <-chan Snapshot,
	matches func(Snapshot) bool,
) Snapshot {
	t.Helper()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		select {
		case snapshot := <-snapshots:
			if matches(snapshot) {
				return snapshot
			}
		case <-timer.C:
			t.Fatal("timed out waiting for receiver snapshot")
		}
	}
}
