package scenario

type streamStatus uint8

const (
	streamRunning streamStatus = iota
	streamPaused
)

type streamState struct {
	targetRate int
	status     bool
}
