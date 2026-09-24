package engine

import (
	"context"
	"fmt"
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
		fmt.Printf("[SCHEDULER] Seconds until event is (%.2fs s)\n", timeLeft.Seconds())

		elapsed := time.Since(start)

		fmt.Printf(
			"[SCHEDULER] elapsed: %s | time left: %s\n",
			elapsed.Truncate(time.Millisecond),
			timeLeft.Truncate(time.Millisecond),
		)

		if timeLeft > 0 {
			// Create non blocking timer
			timer := time.NewTimer(timeLeft)

			fmt.Printf(
				"[SCHEDULER] waiting for event %q at %s\n",
				event.Action,
				event.At,
			)

			select {
			case <-ctx.Done():
				timer.Stop()
				fmt.Printf("[SCHEDULER] context cancelled: %v\n", ctx.Err())
				return nil
			case <-timer.C:
				fmt.Printf(
					"[SCHEDULER] timer fired for %q at elapsed=%s\n",
					event.Action,
					time.Since(start).Truncate(time.Millisecond),
				)
			}
		}

		fmt.Printf(
			"[SCHEDULER] sending %q to stream %q\n",
			event.Action,
			event.Stream,
		)

		// Time to send event
		select {
		case <-ctx.Done():
			fmt.Printf("[SCHEDULER] cancelled before event send\n")
			return nil
		case eventChannels[event.Stream] <- event:
			// Write to the respective event channel when the recieving end is ready to read
			fmt.Printf(
				"[SCHEDULER] sent %q to stream %q\n",
				event.Action,
				event.Stream,
			)
		}

	}
	return nil
}
