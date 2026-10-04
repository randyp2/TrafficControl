package engine

import (
	"fmt"
	"time"
)

// maxCatchUpWindow defines the maximum duration of accumulated delay the pacer
// will attempt to compensate for in a single burst. If the sender falls behind
// by more than this time window, older missed packets are discarded to prevent
// overwhelming the network or OS buffers.
const maxCatchUpWindow = 5 * time.Millisecond

// boundedPacer holds an ongoing timer used to determine how many packets must be sent
type boundedPacer struct {
	timer    *time.Timer
	rate     int
	interval time.Duration
	next     time.Time
	running  bool
}

func newBoundedPacer(
	rate int,
	startedAt time.Time,
) (*boundedPacer, error) {
	interval, err := intervalForRate(rate)

	if err != nil {
		return nil, err
	}

	// Return bounded pacer and start timer
	return &boundedPacer{
		timer:    time.NewTimer(interval),
		rate:     rate,
		interval: interval,
		next:     startedAt.Add(interval),
		running:  true,
	}, nil
}

// intervalForRate finds the interval between packets based on rate
func intervalForRate(rate int) (time.Duration, error) {
	if rate <= 0 {
		return 0, fmt.Errorf(
			"[PACER] rate must be greater than 0: %d\n",
			rate,
		)
	}

	// Calculate time between sending packets
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		return 0, fmt.Errorf(
			"[PACER] rate is too high: %d\n",
			rate,
		)
	}

	return interval, nil
}

func (p *boundedPacer) C() <-chan time.Time {
	return p.timer.C
}

func (p *boundedPacer) packetsDue(now time.Time) int {
	if !p.running || now.Before(p.next) {
		return 0
	}

	due := int(now.Sub(p.next)/p.interval) + 1
	maximum := p.maximumBatch()

	if due > maximum {
		due = maximum

		// Discard debt older than the allowed catch up window
		p.next = now.Add(p.interval)
		return due
	}

	p.next = p.next.Add(time.Duration(due) * p.interval)

	return due
}

// maximumBatch calculates the maximum number of packets allowed in a single
// burst based on the target rate and the maxCatchUpWindow
func (p *boundedPacer) maximumBatch() int {
	numerator := int64(p.rate) * maxCatchUpWindow.Nanoseconds()

	// Integer ceiling division
	packets := (numerator + int64(time.Second) - 1) / int64(time.Second)

	if packets < 1 {
		return 1
	}

	return int(packets)
}

// Schedule is in charge of setting a timer based on delay (time to send next packet)
func (p *boundedPacer) Schedule(now time.Time) {
	if !p.running {
		return
	}

	delay := p.next.Sub(now)

	// Behind so fire as quickly as possible
	if delay <= 0 {
		delay = time.Nanosecond
	}

	p.timer.Reset(delay)
}

// SetRate configures a new rate and resets the time
func (p *boundedPacer) SetRate(
	rate int,
	now time.Time,
) error {
	interval, err := intervalForRate(rate)
	if err != nil {
		return err
	}

	p.rate = rate
	p.interval = interval
	p.next = now.Add(interval)

	if p.running {
		p.timer.Reset(interval)
	}

	return nil
}

// Pause stops the timer
func (p *boundedPacer) Pause() {
	if !p.running {
		return
	}

	p.timer.Stop()
	p.running = false
}

// Resume resets the timer to run based off interval and updates next value
func (p *boundedPacer) Resume(now time.Time) {
	if p.running {
		return
	}

	// Resume from now so the paused elapsed time does not become packet debt
	p.next = now.Add(p.interval)
	p.running = true
	p.timer = time.NewTimer(p.interval)
}

func (p *boundedPacer) Stop() {
	p.timer.Stop()
}
