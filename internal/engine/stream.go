package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/tcp"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
	"honnef.co/go/tools/analysis/facts/generated"
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

	// Initialize reporter and snapshot channel to capture timed snapshots
	var snapshotTicker *time.Ticker
	var snapshotC <-chan time.Time

	if reporter != nil {
		snapshotTicker = time.NewTicker(snapshotInterval)
		snapshotC = snapshotTicker.C
		defer snapshotTicker.Stop()
	}

	// Initialize delayed queue
	delayedPackets := newDelayQueue()
	var releaseTimer *time.Timer
	var releaseC <-chan time.Time

	defer func() {
		if releaseTimer != nil {
			releaseTimer.Stop()
		}
	}()

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
			generatedAt := time.Now()
			batchSize := sendPacer.packetsDue(
				time.Now(),
			)

			if state.latency > 0 {
				// Compute actual release time w/ latency
				releaseAt := generatedAt.Add(state.latency)

				for range batchSize {
					delayedPackets.Enqueue(payload, releaseAt)
				}

				releaseTimer, releaseC = armReleaseTimer(
					releaseTimer,
					delayedPackets,
					time.Now(),
				)

			} else {
				for range batchSize {
					if err := sendPayload(
						sender,
						payload,
						&state,
						stream.Name,
					); err != nil {
						return failStream(
							&state,
							reporter,
							stream.Name,
							err,
						)
					}
				}

				if batchSize > 0 {
					state.updatedAt = time.Now()
				}
			}

			sendPacer.Schedule(time.Now())

		case <-releaseC:
			// Delayed packets until releasedAt time should send now
			releasedAt := time.Now()
			releasedAny := false

			for {
				packet, ready := delayedPackets.PopDue(releasedAt)
				if !ready {
					break
				}

				if err := sendPayload(
					sender,
					packet.payload,
					&state,
					stream.Name,
				); err != nil {
					return failStream(
						&state,
						reporter,
						stream.Name,
						err,
					)
				}

				releasedAny = true
			}

			if releasedAny {
				state.updatedAt = time.Now()
			}

			releaseTimer, releaseC = armReleaseTimer(
				releaseTimer,
				delayedPackets,
				time.Now(),
			)

		case <-snapshotC:
			capturedAt := time.Now()

			// Emit snapshot update
			emitUpdate(reporter, UpdateSnapshot, state.snapshot(stream.Name, capturedAt))

		case event := <-events:
			// Scheduled events

			previousState := state.status
			previousRate := state.targetRate
			previousLatency := state.latency

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
			currentLatency := state.latency
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

			// Changed latency
			if previousLatency != currentLatency {
				state.updatedAt = eventTime

				emitUpdate(reporter, UpdateLatencyChanged, state.snapshot(stream.Name, eventTime))
			}
		}
	}
}

func sendPayload(
	streamSender sender,
	payload []byte,
	state *streamState,
	streamName string,
) error {
	if err := streamSender.Send(payload); err != nil {
		return fmt.Errorf(
			"[STREAM] Failed sending payload for stream %q: %w\n",
			streamName,
			err,
		)
	}

	state.packetsSent++
	state.bytesSent += uint64(len(payload))

	return nil
}

// armReleaseTimer resets the timer based on the daly from now to releaseAt
func armReleaseTimer(
	timer *time.Timer,
	queue *delayQueue,
	now time.Time,
) (*time.Timer, <-chan time.Time) {
	packet, exists := queue.Peek()
	if !exists {
		if timer != nil {
			timer.Stop()
		}

		return timer, nil
	}

	delay := max(0, packet.releaseAt.Sub(now))

	if timer == nil {
		timer = time.NewTimer(delay)
	} else {
		timer.Reset(delay)
	}

	return timer, timer.C
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
