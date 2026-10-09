package main

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

// rateSample collects the number of packets sent at a specific snapshot time
type rateSample struct {
	capturedAt  time.Time
	packetsSent uint64
}

// consoleReporter implements report and holds mutex lock preventing jumbled output
type consoleReporter struct {
	mu      sync.Mutex
	output  io.Writer
	samples map[string]rateSample
}

func newConsoleReporter(output io.Writer) *consoleReporter {
	return &consoleReporter{
		output:  output,
		samples: make(map[string]rateSample),
	}
}

func (r *consoleReporter) Report(update engine.StreamUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := update.Snapshot
	fmt.Fprintf(
		r.output,
		"[%s: %s]",
		strings.ToUpper(string(update.Kind)),
		snapshot.Name,
	)

	switch update.Kind {
	case engine.UpdateStarted:
		r.rememberSample(update.Snapshot)

		fmt.Fprintf(
			r.output,
			" rate=%d pkt/s\n",
			snapshot.TargetRate,
		)

	case engine.UpdateSnapshot:
		actualRate := r.actualSendRate(update.Snapshot)

		fmt.Fprintf(
			r.output,
			" status=%s target=%d sends/s actual=%.1f sends/s sends=%d bytes=%d\n",
			snapshot.Status,
			snapshot.TargetRate,
			actualRate,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateRateChanged:
		fmt.Fprintf(
			r.output,
			" rate=%d pkt/s status=%s\n",
			snapshot.TargetRate,
			snapshot.Status,
		)

	case engine.UpdateLatencyChanged:
		fmt.Fprintf(
			r.output,
			" latency=%s status=%s\n",
			snapshot.Latency,
			snapshot.Status,
		)

	case engine.UpdatePaused:
		fmt.Fprintf(
			r.output,
			" packets=%d bytes=%d\n",
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateResumed:
		fmt.Fprintf(
			r.output,
			" rate=%d pkt/s\n",
			snapshot.TargetRate,
		)

	case engine.UpdateStopped:
		fmt.Fprintf(
			r.output,
			" packets=%d bytes=%d\n",
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)
		r.writeSummary(update)

	case engine.UpdateCanceled:
		fmt.Fprintf(
			r.output,
			" packets=%d bytes=%d\n",
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)
		r.writeSummary(update)

	case engine.UpdateFailed:
		fmt.Fprintf(
			r.output,
			" error=%q packets=%d bytes=%d\n",
			snapshot.LastError,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	default:
		fmt.Fprintf(
			r.output,
			" status=%s\n",
			snapshot.Status,
		)
	}
}

func (r *consoleReporter) writeSummary(update engine.StreamUpdate) {
	snapshot := update.Snapshot
	duration := snapshot.CapturedAt.Sub(snapshot.StartedAt)
	duration = max(duration, 0)

	var averageRate float64
	if duration > 0 {
		averageRate = float64(snapshot.PacketsSent) / duration.Seconds()
	}

	fmt.Fprintf(
		r.output,
		"[SUMMARY: %s] reason=%s duration=%.3fs target=%d packets/s packets=%d bytes=%d average=%.1f packets/s\n",
		snapshot.Name,
		update.Kind,
		duration.Seconds(),
		snapshot.TargetRate,
		snapshot.PacketsSent,
		snapshot.BytesSent,
		averageRate,
	)
}

// rememberSample records packetSent at given captured timestamp
func (r *consoleReporter) rememberSample(
	snapshot engine.StreamSnapshot,
) {
	r.samples[snapshot.Name] = rateSample{
		capturedAt:  snapshot.CapturedAt,
		packetsSent: snapshot.PacketsSent,
	}
}

// actualSendRate calculates the send rate based on previously recorded snapshot
// sendRate = (packetsSent - previousPacketsSent) / (current_time - previous_time)
func (r *consoleReporter) actualSendRate(
	snapshot engine.StreamSnapshot,
) float64 {
	current := rateSample{
		capturedAt:  snapshot.CapturedAt,
		packetsSent: snapshot.PacketsSent,
	}

	// --- Retrieve the previous captured metric
	previous, exists := r.samples[snapshot.Name]
	r.samples[snapshot.Name] = current // Update last recorded

	if !exists {
		return 0
	}

	elapsed := current.capturedAt.Sub(
		previous.capturedAt,
	).Seconds()

	if elapsed <= 0 {
		return 0
	}

	if current.packetsSent < previous.packetsSent {
		return 0
	}

	sent := current.packetsSent - previous.packetsSent

	return float64(sent) / elapsed
}
