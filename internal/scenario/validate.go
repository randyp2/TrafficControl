package scenario

import (
	"errors"
	"fmt"
)

// Validate is a method reciever that ensures the given scenario is "valid"
func (s Scenario) Validate() error {
	if s.Name == "" {
		return errors.New("scenario name is requried")
	}

	if len(s.Streams) == 0 {
		return errors.New("at least one stream must be defined")
	}

	for i, stream := range s.Streams {
		if err := stream.Validate(); err != nil {
			return fmt.Errorf("stream %d: %w", i, err)
		}
	}

	return nil
}

// Validate is a method reciever for Stream that validates config
func (s Stream) Validate() error {
	if s.Name == "" {
		return errors.New("name for stream cannot be empty")
	}

	switch s.Protocol {
	case ProtocolUDP:
	case ProtocolHTTP:
	case ProtocolTCP:
	default:
		return fmt.Errorf("unsupported protocol:%q", s.Protocol)
	}

	if s.Target == "" {
		return errors.New("target is required")
	}

	if s.Rate <= 0 {
		return errors.New("rate must be > 0")
	}

	return nil
}
