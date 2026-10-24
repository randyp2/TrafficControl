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

	// Bidirectional channel to read and write results to
	// Initialize with buffer size so writer threads don't block
	results := make(chan error, len(s.Streams))

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

	start := time.Now()
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
			)
		}(stream, events)
	}

	go scheduleEvents(ctx, start, s.Events, eventChannels, debugLogger, stopStreams)

	var firstErr error
	for range s.Streams {
		err := <-results

		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	return firstErr
}
