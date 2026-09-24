package scenario

// Scenario describes the traffic scenario you are trying to simulate
// Tag with backticks so yaml parser can map config file onto this struct
type Scenario struct {
	Name    string   `yaml:"name"`
	Streams []Stream `yaml:"streams"`
	Events  []Event  `yaml:"events"`
}
