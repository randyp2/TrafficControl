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
	err := listenCommand([]string{"--protocol", "sctp"})
	if err == nil {
		t.Fatal("listenCommand() error = nil, want an unsupported protocol error")
	}

	if !strings.Contains(err.Error(), `unsupported receiver protocol "sctp"`) {
		t.Fatalf("listenCommand() error = %q", err)
	}
}

func TestListenCommandRoutesTCP(t *testing.T) {
	err := listenCommand([]string{"--protocol", "tcp", "--address", "bad:address:format"})
	if err == nil || !strings.Contains(err.Error(), "resolve TCP receiver address") {
		t.Fatalf("listenCommand error = %v, want a TCP address validation error", err)
	}
}

func TestReceiverConsoleReporterFormatsTCPBytes(t *testing.T) {
	startedAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	for _, color := range []bool{false, true} {
		var output bytes.Buffer
		reporter := newReceiverConsoleReporter(&output)
		reporter.color = color
		snapshot := receiver.Snapshot{
			Protocol: receiver.ProtocolTCP, Address: "127.0.0.1:6000",
			BytesReceived: 100, ConnectionsAccepted: 2, ActiveConnections: 1,
			StartedAt: startedAt, CapturedAt: startedAt.Add(time.Second),
		}
		reporter.Report(snapshot)
		lines := strings.Split(strings.TrimSpace(output.String()), "\n")
		want := "1.00s 127.0.0.1:6000 100.0 2 1 0 100"
		if got := strings.Join(strings.Fields(lines[len(lines)-1]), " "); got != want {
			t.Fatalf("TCP row = %q, want %q", got, want)
		}
		if strings.Contains(output.String(), "DATAGRAMS") {
			t.Fatal("TCP output counts read calls as datagrams")
		}
		output.Reset()
		snapshot.CapturedAt = startedAt.Add(2 * time.Second)
		snapshot.BytesReceived = 180
		snapshot.ActiveConnections = 0
		snapshot.ConnectionErrors = 1
		snapshot.Final = true
		reporter.Report(snapshot)
		for _, want := range []string{
			"80.0", "[SUMMARY: tcp]", "Received: 180 bytes | Connections: 2 | Active: 0 | Read errors: 1",
		} {
			if !strings.Contains(output.String(), want) {
				t.Fatalf("TCP summary = %q, missing %q", output.String(), want)
			}
		}
		if strings.Contains(output.String(), "datagrams") {
			t.Fatal("TCP summary reports datagrams")
		}
	}
}
