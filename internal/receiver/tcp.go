package receiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// TCPReceiver counts received bytes across concurrent TCP connections.
type TCPReceiver struct {
	listener       *net.TCPListener
	reportInterval time.Duration
}

// ListenTCP binds a TCP listener. A nonpositive report interval defaults to one second.
func ListenTCP(address string, reportInterval time.Duration) (*TCPReceiver, error) {
	if address == "" {
		return nil, errors.New("TCP receiver address is required")
	}
	if reportInterval <= 0 {
		reportInterval = defaultReportInterval
	}
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve TCP receiver address: %w", err)
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for TCP traffic: %w", err)
	}
	return &TCPReceiver{listener: listener, reportInterval: reportInterval}, nil
}

// Address returns the receiver's bound address.
func (r *TCPReceiver) Address() string {
	return r.listener.Addr().String()
}

// Run accepts connections until cancellation, closes active sockets, then reports final totals.
func (r *TCPReceiver) Run(ctx context.Context, reporter Reporter) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer r.listener.Close()

	startedAt := time.Now()
	address := r.Address()
	var totals counters
	stopReporting := startReporting(ctx, ProtocolTCP, address, startedAt, r.reportInterval, &totals, reporter)
	stopClose := context.AfterFunc(ctx, func() { _ = r.listener.Close() })
	defer stopClose()

	var readers sync.WaitGroup
	var runErr error
	for {
		conn, err := r.listener.AcceptTCP()
		if err != nil {
			if ctx.Err() == nil {
				runErr = fmt.Errorf("accept TCP connection: %w", err)
			}
			break
		}
		totals.connections.Add(1)
		totals.active.Add(1)
		readers.Add(1)
		go func() {
			defer readers.Done()
			receiveTCP(ctx, conn, &totals)
		}()
	}

	cancel()
	readers.Wait()
	stopReporting()
	if reporter != nil {
		reporter.Report(totals.snapshot(ProtocolTCP, address, startedAt, time.Now(), true))
	}
	return runErr
}

func receiveTCP(ctx context.Context, conn *net.TCPConn, totals *counters) {
	defer totals.active.Add(-1)
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()

	buffer := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buffer)
		// TCP reads can contain partial or multiple payloads; only bytes have stable meaning.
		totals.bytes.Add(uint64(n))
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) {
				totals.errors.Add(1)
			}
			return
		}
	}
}
