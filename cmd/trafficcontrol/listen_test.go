package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/receiver"
)

func TestReceiverConsoleReporterFormatsSnapshots(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)

	var output bytes.Buffer
	reporter := newReceiverConsoleReporter(&output)

	reporter.Report(receiver.Snapshot{
		Protocol:          receiver.ProtocolUDP,
		Address:           "127.0.0.1:5000",
		DatagramsReceived: 10,
		BytesReceived:     30,
		StartedAt:         startedAt,
		CapturedAt:        startedAt.Add(time.Second),
	})

	want := "[RECEIVER: udp] address=127.0.0.1:5000 actual=10.0 datagrams/s bytes=30.0/s total_datagrams=10 total_bytes=30\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}

	output.Reset()

	reporter.Report(receiver.Snapshot{
		Protocol:          receiver.ProtocolUDP,
		Address:           "127.0.0.1:5000",
		DatagramsReceived: 18,
		BytesReceived:     54,
		StartedAt:         startedAt,
		CapturedAt:        startedAt.Add(2 * time.Second),
		Final:             true,
	})

	want = "[RECEIVER_STOPPED: udp] address=127.0.0.1:5000 actual=8.0 datagrams/s bytes=24.0/s total_datagrams=18 total_bytes=54\n" +
		"[SUMMARY: udp] address=127.0.0.1:5000 datagrams=18 bytes=54\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestListenCommandRejectsUnsupportedProtocol(t *testing.T) {
	err := listenCommand([]string{"--protocol", "tcp"})
	if err == nil {
		t.Fatal("listenCommand() error = nil, want an unsupported protocol error")
	}

	if !strings.Contains(err.Error(), `unsupported receiver protocol "tcp"`) {
		t.Fatalf("listenCommand() error = %q", err)
	}
}
