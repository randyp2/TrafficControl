package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
)

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
		// Wait for messages to appear in channel
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			if err := sender.Send(payload); err != nil {
				return fmt.Errorf("send stream %q: %w", stream.Name, err)
			}
		}
	}
}
