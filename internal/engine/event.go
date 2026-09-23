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
) (bool, error) {
	switch event.Action {
	case scenario.ActionSetRate:
		// Reset packet rate
		interval := time.Second / time.Duration(event.Rate)
		ticker.Reset(interval)

		return false, nil
	case scenario.ActionStop:
		return true, nil

	default:
		return false, fmt.Errorf("unknown event action type %q", event.Action)
	}
}
