package scenario

import "time"

type Protocol string
type Action string

const (
	// ProtocolUDP represents UDP traffic
	ProtocolUDP Protocol = "udp"

	// ProtocolTCP represents TCP traffic
	ProtocolTCP Protocol = "tcp"
)

// Scenario describes the traffic scenario you are trying to simulate
// Tag with backticks so yaml parser can map config file onto this struct
type Scenario struct {
	Name    string   `yaml:"name"`
	Streams []Stream `yaml:"streams"`
	Event   []Event  `yaml:"events"`
}

// Steram describes a configured source of generated network traffic
type Stream struct {
	Name     string   `yaml:"name"`
	Protocol Protocol `yaml:"protocol"`
	Target   string   `yaml:"target"`
	Rate     int      `yaml:"rate"`
	Payload  string   `yaml:"payload"`
}

type Event struct {
	At     time.Duration `yaml:"at"`
	Stream string        `yaml:"stream"`
	Action Action        `yaml:"action"`
	Rate   int           `yaml:"rate,omitempty"`
}
