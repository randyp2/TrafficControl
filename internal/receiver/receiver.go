package receiver

import (
	"sync/atomic"
	"time"
)

// Protocol defines a supported receiver protocol
type Protocol string

const (
	// ProtcolUDP listens for UDP datagrams
	ProtcolUDP Protocol = "udp"
)

// Snapshot collects metrics based on what the receiver has observed
type Snapshot struct {
	Protocol          Protocol
	Address           string
	DatagramsReceived uint64
	BytesReceived     uint64
	StartedAt         time.Time
	CapturedAt        time.Time
	Final             bool
}

type Reporter interface {
	Report(Snapshot)
}

// Thread safe counters to handle goroutines
type counters struct {
	datagrams atomic.Uint64
	bytes     atomic.Uint64
}

func (c *counters) snapshot(
	address string,
	startedAt time.Time,
	capturedAt time.Time,
	final bool,
) Snapshot {
	return Snapshot{
		Protocol:          ProtcolUDP,
		Address:           address,
		DatagramsReceived: c.datagrams.Load(),
		BytesReceived:     c.bytes.Load(),
		StartedAt:         startedAt,
		CapturedAt:        capturedAt,
		Final:             final,
	}
}
