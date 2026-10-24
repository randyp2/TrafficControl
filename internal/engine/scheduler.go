package engine

import (
	"context"
	"io"
	"log"
	"sort"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func scheduleEvents(
	ctx context.Context,
	start time.Time,
	events []scenario.Event,
	eventChannels map[string]chan scenario.Event,
	debugLogger *log.Logger,
	stopStreams map[string]context.CancelCauseFunc,
) error {
	if debugLogger == nil {
		debugLogger = log.New(io.Discard, "", 0)
	}

	// Copy to not mutate original events slice
	scheduled := append([]scenario.Event(nil), events...)

	// Sort to schedule earlier events first
	sort.SliceStable(scheduled, func(i, j int) bool {
		return scheduled[i].At < scheduled[j].At
	})

	for _, event := range scheduled {
		// Calculate time between now and target time
		timeLeft := time.Until(start.Add(event.At))
		debugLogger.Printf("[SCHEDULER] Seconds until event is (%.2fs)\n", timeLeft.Seconds())

		elapsed := time.Since(start)

		debugLogger.Printf(
			"[SCHEDULER] elapsed: %s | time left: %s\n",
			elapsed.Truncate(time.Millisecond),
			timeLeft.Truncate(time.Millisecond),
		)

		// Time left before executing event
		if timeLeft > 0 {
			// Create non blocking timer
			timer := time.NewTimer(timeLeft)

			debugLogger.Printf(
				"[SCHEDULER] waiting for event %q at %s\n",
				event.Action,
				event.At,
			)

			select {
			case <-ctx.Done():
				timer.Stop()
				debugLogger.Printf("[SCHEDULER] context cancelled: %v\n", ctx.Err())
				return nil

			case <-timer.C:
				debugLogger.Printf(
					"[SCHEDULER] timer fired for %q at elapsed=%s\n",
					event.Action,
					time.Since(start).Truncate(time.Millisecond),
				)
			}
		}

		debugLogger.Printf(
			"[SCHEDULER] sending %q to stream %q\n",
			event.Action,
			event.Stream,
		)

		// Time to send event
		if event.Action == scenario.ActionStop && stopStreams[event.Stream] != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Interrupt blocked writes without waiting for the stream's event loop.
			stopStreams[event.Stream](errScheduledStop)
			debugLogger.Printf("[SCHEDULER] stopped stream %q\n", event.Stream)
			continue
		}

		select {
		case <-ctx.Done():
			debugLogger.Printf("[SCHEDULER] canceled before event send\n")
			return nil

		case eventChannels[event.Stream] <- event:
			// Write to the respective event channel when the recieving end is ready to read
			debugLogger.Printf(
				"[SCHEDULER] sent %q to stream %q\n",
				event.Action,
				event.Stream,
			)
		}

	}
	return nil
}
