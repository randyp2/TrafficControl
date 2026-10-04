package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/tcp"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
)

const snapshotInterval = time.Second

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
	reporter Reporter,
) error {
	sender, err := newSender(stream)
	if err != nil {
		return err
	}

	// Close socket at the end of function lifecycle
	defer sender.Close()

	now := time.Now()
	sendPacer, err := newBoundedPacer(stream.Rate, now)

	if err != nil {

		return fmt.Errorf(
			"[STREAM]: failed creating a pace for stream %q: %w\n",
			stream.Name,
			err,
		)
	}
	defer sendPacer.Stop()

	state := streamState{
		targetRate: stream.Rate,
		status:     streamRunning,

		startedAt: now,
		updatedAt: now,
	}
	emitUpdate(reporter, UpdateStarted, state.snapshot(stream.Name, now))

	var snapshotTicker *time.Ticker
	var snapshotC <-chan time.Time

	if reporter != nil {
		snapshotTicker = time.NewTicker(snapshotInterval)
		snapshotC = snapshotTicker.C
		defer snapshotTicker.Stop()
	}

	payload := []byte(stream.Payload)
	for {
		select {
		case <-ctx.Done():
			// Cancel method invoked

			canceledAt := time.Now()
			state.status = streamStopped
			state.updatedAt = canceledAt

			emitUpdate(reporter, UpdateCanceled, state.snapshot(stream.Name, canceledAt))

			return nil

		case <-sendPacer.C():
			// Timed event from pacer occured
			batchSize := sendPacer.packetsDue(
				time.Now(),
			)

			for range batchSize {
				if err := sender.Send(payload); err != nil {
					sendErr := fmt.Errorf(
						"[STREAM] Failed sending payload for stream %q: %w\n",
						stream.Name,
						err,
					)

					return failStream(
						&state,
						reporter,
						stream.Name,
						sendErr,
					)
				}

				state.packetsSent++
				state.bytesSent += uint64(len(payload))
			}

			sentAt := time.Now()
			state.updatedAt = sentAt
			sendPacer.Schedule(sentAt)

		case <-snapshotC:
			capturedAt := time.Now()

			// Emit snapshot update
			emitUpdate(reporter, UpdateSnapshot, state.snapshot(stream.Name, capturedAt))

		case event := <-events:
			// Scheduled events

			previousState := state.status
			previousRate := state.targetRate

			stop, err := handleEvent(sendPacer, event, &state)
			if err != nil {
				eventErr := fmt.Errorf(
					"handle event for stream %q: %w",
					stream.Name,
					err,
				)

				return failStream(&state, reporter, stream.Name, eventErr)
			}

			currentState := state.status
			currentRate := state.targetRate
			eventTime := time.Now()

			if stop {
				state.status = streamStopped
				state.updatedAt = eventTime

				emitUpdate(reporter, UpdateStopped, state.snapshot(stream.Name, eventTime))

				return nil
			}

			// Changed the current stream state
			if previousState != currentState {
				state.updatedAt = eventTime

				switch state.status {
				case streamRunning:
					emitUpdate(reporter, UpdateResumed, state.snapshot(stream.Name, eventTime))
				case streamPaused:
					emitUpdate(reporter, UpdatePaused, state.snapshot(stream.Name, eventTime))
				}
			}

			// Changed packet send rate
			if previousRate != currentRate {
				state.updatedAt = eventTime

				emitUpdate(reporter, UpdateRateChanged, state.snapshot(stream.Name, eventTime))
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

func failStream(
	state *streamState,
	reporter Reporter,
	streamName string,
	err error,
) error {
	failedAt := time.Now()

	state.status = streamFailed
	state.updatedAt = failedAt
	state.lastError = err.Error()

	emitUpdate(reporter, UpdateFailed, state.snapshot(streamName, failedAt))

	return err
}
