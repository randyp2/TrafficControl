package receiver

import (
	"context"
	"sync/atomic"
	"time"
)

const defaultReportInterval = time.Second

// Protocol defines a supported receiver protocol
type Protocol string

const (
	// ProtocolUDP listens for UDP datagrams
	ProtocolUDP Protocol = "udp"
	// ProtocolTCP listens for TCP byte streams.
	ProtocolTCP Protocol = "tcp"
)

// Receiver listens for traffic until cancellation and reports a final snapshot.
type Receiver interface {
	Address() string
	Run(context.Context, Reporter) error
}

// Snapshot collects metrics based on what the receiver has observed
type Snapshot struct {
	Protocol            Protocol
	Address             string
	DatagramsReceived   uint64
	BytesReceived       uint64
	ConnectionsAccepted uint64
	ActiveConnections   int64
	ConnectionErrors    uint64
	StartedAt           time.Time
	CapturedAt          time.Time
	Final               bool
}

// Reporter receives snapshots from a traffic receiver.
type Reporter interface {
	Report(Snapshot)
}

// Thread safe counters to handle goroutines
type counters struct {
	datagrams   atomic.Uint64
	bytes       atomic.Uint64
	connections atomic.Uint64
	active      atomic.Int64
	errors      atomic.Uint64
}

func (c *counters) snapshot(
	protocol Protocol,
	address string,
	startedAt time.Time,
	capturedAt time.Time,
	final bool,
) Snapshot {
	return Snapshot{
		Protocol:            protocol,
		Address:             address,
		DatagramsReceived:   c.datagrams.Load(),
		BytesReceived:       c.bytes.Load(),
		ConnectionsAccepted: c.connections.Load(),
		ActiveConnections:   c.active.Load(),
		ConnectionErrors:    c.errors.Load(),
		StartedAt:           startedAt,
		CapturedAt:          capturedAt,
		Final:               final,
	}
}

// startReporing reports periodic snapshots returns a function to stop reporting loop
func startReporting(
	ctx context.Context,
	protocol Protocol,
	address string,
	startedAt time.Time,
	interval time.Duration,
	totals *counters,
	reporter Reporter,
) func() {
	if reporter == nil {
		return func() {}
	}

	reportCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}) // Empty channel used for graceful shutdown

	// Background go routine to reporter snapshots
	go func() {
		// Clean up done (unblocks <-done operations)
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-reportCtx.Done():
				return

			case <-ticker.C:
				capturedAt := time.Now()
				reporter.Report(
					totals.snapshot(protocol, address, startedAt, capturedAt, false),
				)
			}
		}
	}()

	return func() {
		cancel() // Invokes close(done)
		<-done   // Blocks until startReporting is fully cleaned up
	}
}
