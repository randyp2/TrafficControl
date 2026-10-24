package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/randyp2/trafficcontrol/internal/receiver"
)

// receiverSample represents a "point-in-time" sample
// typically the previous sample in this use case
type receiverSample struct {
	capturedAt time.Time
	datagrams  uint64
	bytes      uint64
}

// receiverConsoleReporter is responsible for outputing reports based on snapshots
type receiverConsoleReporter struct {
	mu       sync.Mutex
	output   io.Writer
	previous *receiverSample
	color    bool
	header   bool
}

var _ receiver.Reporter = (*receiverConsoleReporter)(nil)

func newReceiverConsoleReporter(output io.Writer) *receiverConsoleReporter {
	return &receiverConsoleReporter{
		output: output,
		color:  consoleColor(output),
	}
}

// Report prints out receiver metrics to the io.Writer
func (r *receiverConsoleReporter) Report(snapshot receiver.Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// --- Instantiate previous snapshots
	previous := receiverSample{
		capturedAt: snapshot.StartedAt,
	}

	if r.previous != nil {
		previous = *r.previous
	}

	elapsed := snapshot.CapturedAt.Sub(
		previous.capturedAt,
	).Seconds()

	var datagramsPerSecond float64
	var bytesPerSecond float64

	// --- Calculate metric
	if elapsed > 0 {
		datagrams := snapshot.DatagramsReceived - previous.datagrams
		bytes := snapshot.BytesReceived - previous.bytes

		datagramsPerSecond = float64(datagrams) / elapsed
		bytesPerSecond = float64(bytes) / elapsed
	}

	var output strings.Builder
	if !r.header {
		fmt.Fprintf(&output, "\n%s receiver | Rates measured over each reporting interval\n", strings.ToUpper(string(snapshot.Protocol)))
		var columns string
		if snapshot.Protocol == receiver.ProtocolTCP {
			columns = fmt.Sprintf("%8s %-22s %12s %12s %8s %8s %12s\n",
				"TIME", "ADDRESS", "BYTES/s", "CONNECTIONS", "ACTIVE", "ERRORS", "BYTES")
		} else {
			columns = fmt.Sprintf("%8s %-22s %12s %12s %12s %12s\n",
				"TIME", "ADDRESS", "RECEIVED/s", "BYTES/s", "DATAGRAMS", "BYTES")
		}
		output.WriteString(colorize(strings.TrimSuffix(columns, "\n"), colorCyan, r.color) + "\n")
		r.header = true
	}

	if snapshot.Protocol == receiver.ProtocolTCP {
		fmt.Fprintf(&output, "%7.2fs %-22s %12.1f %12d %8d %8d %12d\n",
			max(snapshot.CapturedAt.Sub(snapshot.StartedAt), 0).Seconds(),
			snapshot.Address, bytesPerSecond, snapshot.ConnectionsAccepted,
			snapshot.ActiveConnections, snapshot.ConnectionErrors, snapshot.BytesReceived)
	} else {
		fmt.Fprintf(
			&output,
			"%7.2fs %-22s %12.1f %12.1f %12d %12d\n",
			max(snapshot.CapturedAt.Sub(snapshot.StartedAt), 0).Seconds(),
			snapshot.Address,
			datagramsPerSecond,
			bytesPerSecond,
			snapshot.DatagramsReceived,
			snapshot.BytesReceived,
		)
	}

	if snapshot.Final {
		fmt.Fprintf(&output, "\n%s %s\n",
			colorize(fmt.Sprintf("[SUMMARY: %s]", snapshot.Protocol), colorCyan, r.color), snapshot.Address)
		if snapshot.Protocol == receiver.ProtocolTCP {
			fmt.Fprintf(&output, "  Received: %d bytes | Connections: %d | Active: %d | Read errors: %d\n\n",
				snapshot.BytesReceived, snapshot.ConnectionsAccepted, snapshot.ActiveConnections, snapshot.ConnectionErrors)
		} else {
			fmt.Fprintf(&output, "  Received: %d datagrams | Bytes: %d\n\n",
				snapshot.DatagramsReceived, snapshot.BytesReceived)
		}
	}
	io.WriteString(r.output, output.String())

	r.previous = &receiverSample{
		capturedAt: snapshot.CapturedAt,
		datagrams:  snapshot.DatagramsReceived,
		bytes:      snapshot.BytesReceived,
	}
}

// listenCommand registers a listen subcommand and its respective flags
func listenCommand(args []string) error {
	flags := flag.NewFlagSet(
		"listen",
		flag.ContinueOnError,
	)
	flags.SetOutput(os.Stderr)

	// "--protocol" flag
	protocol := flags.String(
		"protocol",
		"udp",
		"protocol to receive (udp or tcp)",
	)

	// "--address flag"
	address := flags.String(
		"address",
		":5000",
		"local address to listen on",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	// --- Usage error
	if flags.NArg() != 0 {
		return fmt.Errorf(
			"usage: trafficcontrol listen --protocol <udp|tcp> --address :5000",
		)
	}

	var trafficReceiver receiver.Receiver
	var err error
	switch receiver.Protocol(*protocol) {
	case receiver.ProtocolUDP:
		trafficReceiver, err = receiver.ListenUDP(*address, time.Second)
	case receiver.ProtocolTCP:
		trafficReceiver, err = receiver.ListenTCP(*address, time.Second)
	default:
		return fmt.Errorf(
			"unsupported receiver protocol %q",
			*protocol,
		)
	}

	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stdout,
		"%s address=%s\n",
		colorize(fmt.Sprintf("[LISTENING: %s]", *protocol), colorGreen, consoleColor(os.Stdout)),
		trafficReceiver.Address(),
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	reporter := newReceiverConsoleReporter(os.Stdout)

	return trafficReceiver.Run(ctx, reporter)
}
