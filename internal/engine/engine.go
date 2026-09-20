package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
)

func Run(ctx context.Context, s scenario.Scenario) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Bidirectional channel to read and write results to
	// Initialize with buffer size so writer threads don't block
	results := make(chan error, len(s.Streams))

	for _, stream := range s.Streams {
		go func() {
			results <- RunStream(ctx, stream)
		}()
	}

	// Find first possible error, nil if none
	var firstErr error
	for range s.Streams {
		// Pop first message off the channel/queue
		err := <-results

		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	return firstErr
}

// RunStream is the engine that takes a context to manage lifecycle information
// and a stream that contains stream specific configurations to determine
// what payloads to send and when
func RunStream(ctx context.Context, stream scenario.Stream) error {
	switch stream.Protocol {
	case scenario.ProtocolUDP:
		return runUDPStream(ctx, stream)
	default:
		return fmt.Errorf("unsupported protocol %q", stream.Protocol)
	}
}

// runUDPStream is an internal method that performs the actual action of
// sending UDP packet based on a timer
func runUDPStream(ctx context.Context, stream scenario.Stream) error {
	sender, err := udp.Dial(stream.Target)
	if err != nil {
		return err
	}

	// Close UDP socket at the end of function lifecycle
	defer sender.Close()

	interval := time.Second / time.Duration(stream.Rate) // Time between packet sends

	// Send timed events to ticker.C channel
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	payload := []byte(stream.Payload)
	for {
		select {
		case <-ctx.Done():
			// Cancel method invoked
			return nil

		case <-ticker.C:
			// Timed event occured
			if err := sender.Send(payload); err != nil {
				return fmt.Errorf("send stream %q: %w", stream.Name, err)
			}
		}
	}
}
