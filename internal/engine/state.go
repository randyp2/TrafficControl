package engine

type streamStatus uint8

const (
	streamRunning streamStatus = iota
	streamPaused
)

type streamState struct {
	targetRate int
	status     streamStatus
}
