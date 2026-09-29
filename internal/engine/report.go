package engine

import (
	"time"
)

// StreamStatus is the current runtime status of a stream
type StreamStatus string

const (
	StreamStatusRunning StreamStatus = "running"
	StreamStatusPaused  StreamStatus = "paused"
	StreamStatusStopped StreamStatus = "stopped"
	StreamStatusFailed  StreamStatus = "failed"
)

// UpdateKind explains why a stream event was emitted
type UpdateKind string

const (
	UpdateStarted     UpdateKind = "started"
	UpdateSnapshot    UpdateKind = "snapshot"
	UpdateRateChanged UpdateKind = "rate_changed"
	UpdatePaused      UpdateKind = "paused"
	UpdateResume      UpdateKind = "resume"
	UpdateStopped     UpdateKind = "stopped"
	UpdateCanceled    UpdateKind = "canceled"
	UpdateFailed      UpdateKind = "failed"
)

// StreamSnapshot represents a "point-in-time" snapshot view of a stream
type StreamSnapshot struct {
	Name        string
	Status      StreamStatus
	TargetRate  int
	PacketsSent uint64
	BytesSent   uint64
	StartedAt   time.Time
	UpdatedAt   time.Time
	LastErorr   string
}

// StreamUpdate contains snapshot and why its emitted
type StreamUpdate struct {
	Kind     UpdateKind
	Snapshot StreamSnapshot
}

type Reporter interface {
	Report(StreamUpdate)
}

func emitUpdate(reporter Reporter, kind UpdateKind, snapshot StreamSnapshot) {
	if reporter == nil {
		return
	}

	reporter.Report(StreamUpdate{Kind: kind, Snapshot: snapshot})
}
