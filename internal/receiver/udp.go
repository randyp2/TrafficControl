package receiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	maxUDPDatagramSize = 64 * 1024
)

// UDPReceiver receives and counts UDP datagrams.
type UDPReceiver struct {
	conn           *net.UDPConn
	reportInterval time.Duration
}

// ListenUDP binds a listener to an address
func ListenUDP(address string, reportInterval time.Duration) (*UDPReceiver, error) {
	if address == "" {
		return nil, errors.New("UDP receiver address is required")
	}

	if reportInterval <= 0 {
		reportInterval = defaultReportInterval
	}

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve UDP receiver address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for UDP traffic: %w", err)
	}

	return &UDPReceiver{
		conn:           conn,
		reportInterval: reportInterval,
	}, nil
}

// Address returns the receiver's bound address.
func (r *UDPReceiver) Address() string {
	return r.conn.LocalAddr().String()
}

// Run runs an infinite loop thats listens on a UDP socket and continuously reports datagrams
func (r *UDPReceiver) Run(
	ctx context.Context,
	reporter Reporter,
) error {
	defer r.conn.Close()

	startedAt := time.Now()
	address := r.Address()
	var totals counters

	stopReporting := startReporting(
		ctx,
		ProtocolUDP,
		address,
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

	buffer := make([]byte, maxUDPDatagramSize)

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
				"read UDP traffic: %w",
				err,
			)
			break
		}

		totals.bytes.Add(uint64(n))
		totals.datagrams.Add(1)
	}

	stopReporting()

	// --- Report last event
	if reporter != nil {
		reporter.Report(
			totals.snapshot(
				ProtocolUDP,
				address,
				startedAt,
				time.Now(),
				true,
			),
		)
	}

	return runErr
}
