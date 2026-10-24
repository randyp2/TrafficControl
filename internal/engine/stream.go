package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
	"github.com/randyp2/trafficcontrol/internal/transport/tcp"
	"github.com/randyp2/trafficcontrol/internal/transport/udp"
)

const snapshotInterval = time.Second

var errScheduledStop = errors.New("scheduled stream stop")

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
	sender, err := newSender(ctx, stream)
	if err != nil {
		if ctx.Err() != nil {
			state := streamState{targetRate: stream.Rate, startedAt: time.Now()}
			return cancelStream(ctx, &state, reporter, stream.Name)
		}
		return err
	}

	// Close socket at the end of function lifecycle
	defer sender.Close()
	// Closing the socket interrupts a write even while the stream loop is blocked.
	stopClose := context.AfterFunc(ctx, func() { _ = sender.Close() })
	defer stopClose()

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
	lossRNG := rand.New(rand.NewSource(stream.Seed))
	for {
		select {
		case <-ctx.Done():
			// Cancel method invoked
			return cancelStream(ctx, &state, reporter, stream.Name)

		case <-sendPacer.C():
			// Timed event from pacer occured
			generatedAt := time.Now()
			releaseAt := generatedAt.Add(state.latency)
			batchSize := sendPacer.packetsDue(
				generatedAt,
			)

			for range batchSize {
				state.packetsGenerated++

				// Drop packets based on loss percentage
				if state.lossPercent > 0 &&
					lossRNG.Float64()*100 < state.lossPercent {
					state.packetsDropped++
					continue
				}

				// Enqueue and forget
				if state.latency > 0 {
					delayedPackets.Enqueue(payload, releaseAt)
					continue
				}

				// Normal send
				if err := sendPayload(
					sender,
					payload,
					&state,
					stream.Name,
				); err != nil {
					if ctx.Err() != nil {
						return cancelStream(ctx, &state, reporter, stream.Name)
					}
					return failStream(
						&state,
						reporter,
						stream.Name,
						err,
					)
				}
			}

			// Set timer to release the delayed packets
			if state.latency > 0 {
				releaseTimer, releaseC = armReleaseTimer(
					releaseTimer,
					delayedPackets,
					time.Now(),
				)
			}

			if batchSize > 0 {
				state.updatedAt = time.Now()
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
					if ctx.Err() != nil {
						return cancelStream(ctx, &state, reporter, stream.Name)
					}
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
			previousLoss := state.lossPercent

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
			currentLoss := state.lossPercent
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

			// Changed loss percent
			if previousLoss != currentLoss {
				state.updatedAt = eventTime

				emitUpdate(reporter, UpdateLossChanged, state.snapshot(stream.Name, eventTime))
			}
		}
	}
}

func cancelStream(ctx context.Context, state *streamState, reporter Reporter, name string) error {
	kind := UpdateCanceled
	if errors.Is(context.Cause(ctx), errScheduledStop) {
		kind = UpdateStopped
	}
	now := time.Now()
	state.status = streamStopped
	state.updatedAt = now
	emitUpdate(reporter, kind, state.snapshot(name, now))
	return nil
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
func newSender(ctx context.Context, stream scenario.Stream) (sender, error) {
	switch stream.Protocol {
	case scenario.ProtocolUDP:
		return udp.Dial(stream.Target)
	case scenario.ProtocolTCP:
		return tcp.Dial(ctx, stream.Target)
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
