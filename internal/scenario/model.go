package scenario

// Protocol refers to the network protocol being used UDP | TCP | Http
type Protocol string

const (
	// ProtocolUDP represents UDP traffic
	ProtocolUDP Protocol = "udp"

	// ProtocolTCP represents TCP traffic
	ProtocolTCP Protocol = "tcp"

	// ProtocolHTTP represents http traffic
	ProtocolHTTP Protocol = "http"
)

// Scenario describes the traffic scenario you are trying to simulate
// Tag with backticks so yaml parser can map config file onto this struct
type Scenario struct {
	Name    string   `yaml:"name"`
	Streams []Stream `yaml:"streams"`
}

// Steram describes a configured source of generated network traffic
type Stream struct {
	Name     string   `yaml:"name"`
	Protocol Protocol `yaml:"protocol"`
	Target   string   `yaml:"target"`
	Rate     int      `yaml:"rate"`
	Payload  string   `yaml:"payload"`
}
