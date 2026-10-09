package engine

import (
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

// handleEvent adjusts ticker based on action type and return true if action
// was to stop
func handleEvent(
	sendPacer *boundedPacer,
	event scenario.Event,
	state *streamState,
) (bool, error) {
	now := time.Now()

	switch event.Action {
	case scenario.ActionSetRate:
		if event.Rate <= 0 {
			return false, fmt.Errorf(
				"rate must be greater than zero: %d",
				event.Rate,
			)
		}

		// Reset packet rate
		interval := time.Second / time.Duration(event.Rate)
		if interval <= 0 {
			return false, fmt.Errorf(
				"rate %d is too high",
				event.Rate,
			)
		}
		state.targetRate = event.Rate

		if err := sendPacer.SetRate(event.Rate, now); err != nil {
			return false, err
		}

		return false, nil

	case scenario.ActionSetLatency:
		if event.Latency < 0 {
			return false, fmt.Errorf(
				"latency cannot be negative: %s\n",
				event.Latency,
			)
		}

		state.latency = event.Latency
		return false, nil

	case scenario.ActionPause:
		// Already paused
		if state.status == streamPaused {
			return false, nil
		}

		sendPacer.Pause()
		state.status = streamPaused
		return false, nil

	case scenario.ActionResume:
		// Already running
		if state.status == streamRunning {
			return false, nil
		}

		interval := time.Second / time.Duration(state.targetRate)
		if interval <= 0 {
			return false, fmt.Errorf(
				"rate %d is too high",
				state.targetRate,
			)
		}
		sendPacer.Resume(now)
		state.status = streamRunning

		return false, nil

	case scenario.ActionStop:
		return true, nil

	default:
		return false, fmt.Errorf("unknown event action type %q", event.Action)
	}
}
