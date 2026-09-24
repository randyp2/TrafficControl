package engine

import (
	"context"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func Run(ctx context.Context, s scenario.Scenario) error {
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

	for _, stream := range s.Streams {
		eventChannels[stream.Name] = make(
			chan scenario.Event,
			len(s.Events),
		)
	}

	start := time.Now()
	for _, stream := range s.Streams {
		events := eventChannels[stream.Name]

		go func() {
			results <- RunStream(ctx, stream, events)
		}()
	}

	go scheduleEvents(ctx, start, s.Events, eventChannels)

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
