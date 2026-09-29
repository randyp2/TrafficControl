package engine

import "time"

type streamStatus uint8

const (
	streamRunning streamStatus = iota
	streamPaused
	streamStopped
	streamFailed
)

type streamState struct {
	targetRate int
	status     streamStatus

	packetsSent uint64
	bytesSent   uint64

	startedAt time.Time
	updatedAt time.Time
	lastError string
}

func (s streamState) snapshot(name string) StreamSnapshot {
	return StreamSnapshot{
		Name:        name,
		Status:      getStreamStatus(s.status),
		TargetRate:  s.targetRate,
		PacketsSent: s.packetsSent,
		BytesSent:   s.bytesSent,
		StartedAt:   s.startedAt,
		UpdatedAt:   s.updatedAt,
		LastErorr:   s.lastError,
	}
}

func getStreamStatus(status streamStatus) StreamStatus {
	switch status {
	case streamPaused:
		return StreamStatusPaused
	case streamStopped:
		return StreamStatusStopped
	case streamFailed:
		return StreamStatusFailed
	default:
		return StreamStatusRunning
	}
}
