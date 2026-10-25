package engine

import (
	"fmt"
	"math"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

const maxDuration = time.Duration(1<<63 - 1)

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

		if event.Latency < state.jitter {
			return false, fmt.Errorf("latency %s cannot be less than active jitter %s", event.Latency, state.jitter)
		}
		if event.Latency > maxDuration-state.jitter {
			return false, fmt.Errorf("latency plus jitter is too large")
		}

		state.latency = event.Latency
		return false, nil

	case scenario.ActionSetJitter:
		if event.Jitter < 0 {
			return false, fmt.Errorf("jitter cannot be negative: %s", event.Jitter)
		}
		if event.Jitter > state.latency {
			return false, fmt.Errorf("jitter %s exceeds latency %s", event.Jitter, state.latency)
		}
		if state.latency > maxDuration-event.Jitter {
			return false, fmt.Errorf("latency plus jitter is too large")
		}
		state.jitter = event.Jitter
		return false, nil

	case scenario.ActionSetLoss:
		if math.IsNaN(event.LossPercent) ||
			event.LossPercent < 0 ||
			event.LossPercent > 100 {
			return false, fmt.Errorf(
				"packet loss must be between 0 to 100: %.2f\n",
				event.LossPercent,
			)
		}

		state.lossPercent = event.LossPercent
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
