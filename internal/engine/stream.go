package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/tcp"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
)

// *udp.Sender and *tcp.Sender satisfy this inteface implicitly
// both implement Send([]byte) and Close()
type sender interface {
	Send([]byte) error
	Close() error
}

// RunStream utilizes the sender to send out bytes to the respective socket
// based on timed events
func RunStream(
	ctx context.Context,
	stream scenario.Stream,
	events <-chan scenario.Event,
) error {
	sender, err := newSender(stream)
	if err != nil {
		return err
	}

	// Close socket at the end of function lifecycle
	defer sender.Close()

	interval := time.Second / time.Duration(stream.Rate) // Time between packet sends

	// Send timed events to ticker.C channel
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	state := streamState{
		targetRate: stream.Rate,
		status:     streamRunning,
	}

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
		case event := <-events:
			fmt.Printf(
				"[STREAM %s] received event %q\n",
				stream.Name,
				event.Action,
			)

			stop, err := handleEvent(ticker, event, &state)
			if err != nil {
				return fmt.Errorf(
					"handle event for stream %q: %w",
					stream.Name,
					err,
				)
			}

			if stop {
				fmt.Printf("[STREAM %s] stopping\n", stream.Name)
				return nil
			}
		}
	}
}

// newSender is internal method that creates the sender based on the stream
// configuration
func newSender(stream scenario.Stream) (sender, error) {
	switch stream.Protocol {
	case scenario.ProtocolUDP:
		return udp.Dial(stream.Target)
	case scenario.ProtocolTCP:
		return tcp.Dial(stream.Target)
	default:
		return nil, fmt.Errorf(
			"unsupported protocol %q",
			stream.Protocol,
		)
	}
}
