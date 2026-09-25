package scenario

import (
	"errors"
	"fmt"
	"time"
)

// Validate is a method reciever that ensures the given scenario is "valid"
func (s Scenario) Validate() error {
	if s.Name == "" {
		return errors.New("scenario name is requried")
	}

	if len(s.Streams) == 0 {
		return errors.New("at least one stream must be defined")
	}

	streamNames := make(map[string]struct{}, len(s.Streams))
	for i, stream := range s.Streams {
		if err := stream.Validate(); err != nil {
			return fmt.Errorf("stream %d: %w\n", i, err)
		}

		if _, exists := streamNames[stream.Name]; exists {
			// Ensure unique stream names
			return fmt.Errorf(
				"stream name [%s] already used\n",
				stream.Name,
			)
		}

		streamNames[stream.Name] = struct{}{}
	}

	for i, event := range s.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("event %d: %w\n", i, err)
		}

		if _, exists := streamNames[event.Stream]; !exists {
			return fmt.Errorf(
				"event does not link with any stream: %s ",
				event.Stream,
			)
		}
	}

	return nil
}

// Validate is a method reciever for Stream that validates config
func (s Stream) Validate() error {
	if s.Name == "" {
		return errors.New("name for stream cannot be empty\n")
	}

	switch s.Protocol {
	case ProtocolUDP:
	case ProtocolTCP:
	default:
		return fmt.Errorf("unsupported protocol:%q\n", s.Protocol)
	}

	if s.Target == "" {
		return errors.New("target is required\n")
	}

	if s.Rate <= 0 {
		return errors.New("rate must be > 0\n")
	}

	return nil
}

// Event is a method reciever for Event to validate singular event
func (e Event) Validate() error {
	if e.At < 0 {
		return errors.New("event time cannot be negative\n")
	}

	if e.Stream == "" {
		return errors.New("event stream name cannot be empty\n")
	}

	switch e.Action {
	case ActionSetRate:
		if e.Rate <= 0 {
			return errors.New("event rate cannot be negative\n")
		}

		interval := time.Second / time.Duration(e.Rate)
		if interval <= 0 {
			return fmt.Errorf("event rate %d is too high\n", e.Rate)
		}

	case ActionPause, ActionResume, ActionStop:
		if e.Rate != 0 {
			return fmt.Errorf(
				"event action %q does not accept a rate",
				e.Action,
			)
		}
	default:
		return fmt.Errorf("unsupported event action type: %q\n", e.Action)

	}

	return nil
}
