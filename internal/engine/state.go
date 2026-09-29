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
