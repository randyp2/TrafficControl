package scenario

import "time"

// Scenario describes a traffic-generation scenario.
type Scenario struct {
	Name    string   `yaml:"name"`
	Streams []Stream `yaml:"streams"`
	Events  []Event  `yaml:"events"`
}

// Protocol identifies the network protocol used by a stream.
type Protocol string

const (
	// ProtocolUDP represents UDP traffic.
	ProtocolUDP Protocol = "udp"

	// ProtocolTCP represents TCP traffic.
	ProtocolTCP Protocol = "tcp"
)

// Stream describes a configured source of generated network traffic.
type Stream struct {
	Name     string   `yaml:"name"`
	Protocol Protocol `yaml:"protocol"`
	Target   string   `yaml:"target"`
	Rate     int      `yaml:"rate"`
	Payload  string   `yaml:"payload"`
}

// Action identifies a scheduled change to a stream.
type Action string

const (
	ActionSetRate Action = "set_rate"
	ActionStop    Action = "stop"
	ActionPause   Action = "pause"
	ActionResume  Action = "resume"
)

// Event describes a scheduled action for a stream.
type Event struct {
	At     time.Duration `yaml:"at"`
	Stream string        `yaml:"stream"`
	Action Action        `yaml:"action"`
	Rate   int           `yaml:"rate,omitempty"`
}
