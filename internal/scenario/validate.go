package scenario

import (
	"errors"
	"fmt"
	"math"
	"sort"
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

	// Validate delay settings in execution order without rearranging the input.
	const maxDuration = time.Duration(1<<63 - 1)
	ordered := append([]Event(nil), s.Events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At < ordered[j].At })
	delays := make(map[string]struct{ latency, jitter time.Duration }, len(s.Streams))
	for _, event := range ordered {
		settings := delays[event.Stream]
		switch event.Action {
		case ActionSetLatency:
			settings.latency = event.Latency
		case ActionSetJitter:
			settings.jitter = event.Jitter
		default:
			continue
		}
		if settings.jitter > settings.latency {
			return fmt.Errorf("event at %s for stream %q: jitter %s exceeds latency %s",
				event.At, event.Stream, settings.jitter, settings.latency)
		}
		if settings.latency > maxDuration-settings.jitter {
			return fmt.Errorf("event at %s for stream %q: latency plus jitter is too large", event.At, event.Stream)
		}
		delays[event.Stream] = settings
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

	if e.Action != ActionSetJitter && e.Jitter != 0 {
		return fmt.Errorf("event action %q does not accept jitter", e.Action)
	}

	switch e.Action {
	case ActionSetRate:
		if e.Rate <= 0 {
			return errors.New("event rate must be > 0\n")
		}

		if e.Latency != 0 {
			return fmt.Errorf("event action %q does not accept latency\n", e.Action)
		}

		if e.LossPercent != 0 {
			return fmt.Errorf("event action %q does not accept loss percent\n", e.Action)
		}

		interval := time.Second / time.Duration(e.Rate)
		if interval <= 0 {
			return fmt.Errorf("event rate %d is too high\n", e.Rate)
		}

	case ActionSetLatency:
		if e.Latency < 0 {
			return errors.New("event latency cannot be negative\n")
		}

		if e.Rate != 0 {
			return fmt.Errorf("event action %q does not accept rate\n", e.Action)
		}

		if e.LossPercent != 0 {
			return fmt.Errorf("event action %q does not accept loss percent\n", e.Action)
		}

	case ActionSetJitter:
		if e.Jitter < 0 {
			return errors.New("event jitter cannot be negative")
		}
		if e.Rate != 0 {
			return fmt.Errorf("event action %q does not accept rate", e.Action)
		}
		if e.Latency != 0 {
			return fmt.Errorf("event action %q does not accept latency", e.Action)
		}
		if e.LossPercent != 0 {
			return fmt.Errorf("event action %q does not accept loss percent", e.Action)
		}

	case ActionSetLoss:
		if math.IsNaN(e.LossPercent) || e.LossPercent < 0 || e.LossPercent > 100 {
			return fmt.Errorf("event loss percentage must be between 0 to 100: %.2f\n", e.LossPercent)
		}

		if e.Rate != 0 {
			return fmt.Errorf("event action %q does not accept rate\n", e.Action)
		}

		if e.Latency != 0 {
			return fmt.Errorf("event action %q does not accept latency\n", e.Action)
		}

	case ActionPause, ActionResume, ActionStop:
		if e.Rate != 0 {
			return fmt.Errorf(
				"event action %q does not accept a rate",
				e.Action,
			)
		}

		if e.Latency != 0 {
			return fmt.Errorf("event action %q does not accept latency\n", e.Action)
		}

		if e.LossPercent != 0 {
			return fmt.Errorf("event action %q does not accept loss percent\n", e.Action)
		}
	default:
		return fmt.Errorf("unsupported event action type: %q\n", e.Action)

	}

	return nil
}
