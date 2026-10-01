package receiver

import (
	"context"
	"fmt"
	"net"
	"time"
)

const defaultReportInterval = time.Second
const maxUdpDatagramSize = 64 * 1024

type UDPReceiver struct {
	conn           *net.UDPConn
	reportInterval time.Duration
}

// ListenUDP binds a listener to an address
func ListenUDP(address string, reportInterval time.Duration) (*UDPReceiver, error) {

	if address == "" {
		return nil, fmt.Errorf("[RECEIVER] address is empty\n")
	}

	if reportInterval <= 0 {
		reportInterval = defaultReportInterval
	}

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("[RECEIVER] resolve UDP receiver address: %w\n", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("[RECEIVER] listen for UDP traffic: %w\n", err)
	}

	return &UDPReceiver{
		conn:           conn,
		reportInterval: reportInterval,
	}, nil
}

// Run runs an infinite loop thats listens on a UDP socket and continuously reports datagrams
func (r *UDPReceiver) Run(
	ctx context.Context,
	reporter Reporter,
) error {
	defer r.conn.Close()

	startedAt := time.Now()
	var totals counters

	stopReporting := startReporting(
		ctx,
		r.conn.LocalAddr().String(),
		startedAt,
		r.reportInterval,
		&totals,
		reporter,
	)

	// If ctx is cancelled close the connection
	stopClose := context.AfterFunc(ctx, func() {
		_ = r.conn.Close()
	})
	defer stopClose()

	buffer := make([]byte, maxUdpDatagramSize)

	var runErr error
	// --- Continuously read from UPD until context is canceled or error
	for {

		n, _, err := r.conn.ReadFromUDP(buffer)

		if err != nil {
			// Cancelled because ctx was canceled
			if ctx.Err() != nil {
				break
			}

			runErr = fmt.Errorf(
				"[RECEIVER UDP] error reading UDP traffic: %w",
				err,
			)
		}

		totals.bytes.Add(uint64(n))
		totals.datagrams.Add(1)
	}

	stopReporting()

	// --- Report last event
	if reporter != nil {
		reporter.Report(
			totals.snapshot(
				r.conn.LocalAddr().String(),
				startedAt,
				time.Now(),
				true,
			),
		)
	}

	return runErr
}

// startReporing reports periodic snapshots returns a function to stop reporting loop
func startReporting(
	ctx context.Context,
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
					totals.snapshot(address, startedAt, capturedAt, false),
				)
			}
		}
	}()

	return func() {
		cancel() // Invokes close(done)
		<-done   // Blocks until startReporting is fully cleaned up
	}
}

