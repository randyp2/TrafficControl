package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
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
}

var _ receiver.Reporter = (*receiverConsoleReporter)(nil)

func newReceiverConsoleReporter(output io.Writer) *receiverConsoleReporter {
	return &receiverConsoleReporter{
		output: output,
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

	kind := "RECEIVER"
	if snapshot.Final {
		kind = "RECEIVER_STOPPED"
	}

	fmt.Fprintf(
		r.output,
		"[%s: udp] address=%s actual=%.1f datagrams/s bytes=%.1f/s total_datagrams=%d total_bytes=%d\n",
		kind,
		snapshot.Address,
		datagramsPerSecond,
		bytesPerSecond,
		snapshot.DatagramsReceived,
		snapshot.BytesReceived,
	)

	if snapshot.Final {
		fmt.Fprintf(
			r.output,
			"[SUMMARY: udp] address=%s datagrams=%d bytes=%d\n",
			snapshot.Address,
			snapshot.DatagramsReceived,
			snapshot.BytesReceived,
		)
	}

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
		"protocol to receive",
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
			"usage: trafficcontrol listen --protocol udp --address :5000",
		)
	}

	if *protocol != string(receiver.ProtocolUDP) {
		return fmt.Errorf(
			"unsupported receiver protocol %q",
			*protocol,
		)
	}

	udpReceiver, err := receiver.ListenUDP(
		*address,
		time.Second,
	)

	if err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stdout,
		"[LISTENING: udp] address=%s\n",
		udpReceiver.Address(),
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	reporter := newReceiverConsoleReporter(os.Stdout)

	return udpReceiver.Run(ctx, reporter)
}
