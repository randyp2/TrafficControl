package engine

import (
	"context"
	"log"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

// Run executes all streams and scheduled events. A nil debugLogger disables diagnostics.
func Run(
	ctx context.Context,
	s scenario.Scenario,
	reporter Reporter,
	debugLogger *log.Logger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	streamCount := len(s.Streams)
	// Bidirectional channel to read and write results to
	// Initialize with buffer size so writer threads don't block
	results := make(chan error, streamCount)

	// ready connections
	ready := make(chan struct{}, streamCount)
	// start timers for streams to use
	starts := make(chan time.Time, streamCount)

	// evenChannels maps a stream name onto an event channel
	eventChannels := make(
		map[string]chan scenario.Event,
		len(s.Streams),
	)
	stopStreams := make(map[string]context.CancelCauseFunc, len(s.Streams))

	for _, stream := range s.Streams {
		eventChannels[stream.Name] = make(
			chan scenario.Event,
			len(s.Events),
		)
	}

	for _, stream := range s.Streams {
		events := eventChannels[stream.Name]
		streamCtx, stop := context.WithCancelCause(ctx)
		stopStreams[stream.Name] = stop

		go func(
			stream scenario.Stream,
			events <-chan scenario.Event,
		) {
			defer stop(nil)

			results <- RunStream(
				streamCtx,
				stream,
				events,
				reporter,
				ready,
				starts,
			)
		}(stream, events)
	}

	readyCount := 0
	completedCount := 0
	var firstErr error

	for completedCount < streamCount {
		select {
		case <-ready:
			readyCount++

			if readyCount == streamCount && ctx.Err() == nil {
				start := time.Now()

				for range s.Streams {
					starts <- start
				}

				go scheduleEvents(
					ctx,
					start,
					s.Events,
					eventChannels,
					debugLogger,
					stopStreams,
				)
			}
		case err := <-results:
			completedCount++

			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		}

	}

	return firstErr
}
