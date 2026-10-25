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
	Seed     int64    `yaml:"seed,omitempty"` // Seed for random num generator
}

// Action identifies a scheduled change to a stream.
type Action string

const (
	ActionSetRate    Action = "set_rate"
	ActionSetLatency Action = "set_latency"
	ActionSetJitter  Action = "set_jitter"
	ActionSetLoss    Action = "set_loss"
	ActionStop       Action = "stop"
	ActionPause      Action = "pause"
	ActionResume     Action = "resume"
)

// Event describes a scheduled action for a stream.
type Event struct {
	At      time.Duration `yaml:"at"`
	Stream  string        `yaml:"stream"`
	Action  Action        `yaml:"action"`
	Rate    int           `yaml:"rate,omitempty"`
	Latency time.Duration `yaml:"latency,omitempty"`
	// Jitter is the symmetric variation around the active latency, such as +/-10ms.
	Jitter      time.Duration `yaml:"jitter,omitempty"`
	LossPercent float64       `yaml:"loss_percent,omitempty"`
}
