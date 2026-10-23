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

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	want := "1.00s 127.0.0.1:5000 10.0 30.0 10 30"
	if got := strings.Join(strings.Fields(lines[len(lines)-1]), " "); got != want {
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

	want = "2.00s 127.0.0.1:5000 8.0 24.0 18 54 [SUMMARY: udp] 127.0.0.1:5000 Received: 18 datagrams | Bytes: 54"
	if got := strings.Join(strings.Fields(output.String()), " "); got != want {
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
