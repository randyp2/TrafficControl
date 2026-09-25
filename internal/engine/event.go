package engine

import (
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

// handleEvent adjusts ticker based on action type and return true if action
// was to stop
func handleEvent(
	ticker *time.Ticker,
	event scenario.Event,
	state *streamState,
) (bool, error) {
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
				state.targetRate,
			)
		}
		state.targetRate = event.Rate

		if state.status == streamRunning {
			ticker.Reset(interval)
		}

		return false, nil

	case scenario.ActionPause:
		// Already paused
		if state.status == streamPaused {
			return false, nil
		}

		ticker.Stop()
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
		ticker.Reset(interval)
		state.status = streamRunning

		return false, nil

	case scenario.ActionStop:
		return true, nil

	default:
		return false, fmt.Errorf("unknown event action type %q", event.Action)
	}
}
