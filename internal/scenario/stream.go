package scenario

type Protocol string

const (
	// ProtocolUDP represents UDP traffic
	ProtocolUDP Protocol = "udp"

	// ProtocolTCP represents TCP traffic
	ProtocolTCP Protocol = "tcp"
)

// Steram describes a configured source of generated network traffic
type Stream struct {
	Name     string   `yaml:"name"`
	Protocol Protocol `yaml:"protocol"`
	Target   string   `yaml:"target"`
	Rate     int      `yaml:"rate"`
	Payload  string   `yaml:"payload"`
}
