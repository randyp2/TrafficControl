package engine

import (
	"context"
	"sort"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func scheduleEvents(
	ctx context.Context,
	start time.Time,
	events []scenario.Event,
	eventChannels map[string]chan scenario.Event,
) error {
	// Copy to not mutate original events slice
	scheduled := append([]scenario.Event(nil), events...)

	// Sort to schedule earlier events first
	sort.SliceStable(scheduled, func(i, j int) bool {
		return scheduled[i].At < scheduled[j].At
	})

	for _, event := range scheduled {
		// Calculate time between now and target time
		timeLeft := time.Until(start.Add(event.At))

		if timeLeft > 0 {
			// Create non blocking timer
			timer := time.NewTimer(timeLeft)

			select {
			case <-ctx.Done():
				return nil
			case <-timer.C:
			}
		}

		// Time to send event
		select {
		case <-ctx.Done():
			return nil
		case eventChannels[event.Stream] <- event:
			// Write to the respective event channel when the recieving end is ready to read
		}

	}
	return nil
}
