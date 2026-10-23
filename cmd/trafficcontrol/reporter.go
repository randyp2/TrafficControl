package main

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

// rateSample collects generated and sent packet counts at a specific snapshot time
type rateSample struct {
	capturedAt       time.Time
	packetsGenerated uint64
	packetsSent      uint64
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
		generatedRate, sentRate := r.actualRates(snapshot)

		fmt.Fprintf(
			r.output,
			" status=%s target=%d packets/s generated_rate=%.1f packets/s actual=%.1f sends/s sends=%d bytes=%d generated=%d dropped=%d loss_percent=%.2f observed_loss_percent=%.2f\n",
			snapshot.Status,
			snapshot.TargetRate,
			generatedRate,
			sentRate,
			snapshot.PacketsSent,
			snapshot.BytesSent,
			snapshot.PacketsGenerated,
			snapshot.PacketsDropped,
			snapshot.LossPercent,
			observedLossPercent(snapshot),
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

	case engine.UpdateLossChanged:
		fmt.Fprintf(
			r.output,
			" loss_percent=%.2f status=%s\n",
			snapshot.LossPercent,
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

	var averageRate, averageGeneratedRate float64
	if duration > 0 {
		averageRate = float64(snapshot.PacketsSent) / duration.Seconds()
		averageGeneratedRate = float64(snapshot.PacketsGenerated) / duration.Seconds()
	}

	fmt.Fprintf(
		r.output,
		"[SUMMARY: %s] reason=%s duration=%.3fs target=%d packets/s packets=%d bytes=%d average=%.1f packets/s generated=%d dropped=%d average_generated=%.1f packets/s loss_percent=%.2f observed_loss_percent=%.2f\n",
		snapshot.Name,
		update.Kind,
		duration.Seconds(),
		snapshot.TargetRate,
		snapshot.PacketsSent,
		snapshot.BytesSent,
		averageRate,
		snapshot.PacketsGenerated,
		snapshot.PacketsDropped,
		averageGeneratedRate,
		snapshot.LossPercent,
		observedLossPercent(snapshot),
	)
}

// rememberSample records generated and sent counts at the captured timestamp
func (r *consoleReporter) rememberSample(
	snapshot engine.StreamSnapshot,
) {
	r.samples[snapshot.Name] = rateSample{
		capturedAt:       snapshot.CapturedAt,
		packetsGenerated: snapshot.PacketsGenerated,
		packetsSent:      snapshot.PacketsSent,
	}
}

// actualRates calculates interval rates from consecutive samples for each stream.
// Loss-change events do not reset the interval baseline.
func (r *consoleReporter) actualRates(
	snapshot engine.StreamSnapshot,
) (generatedPerSecond, sentPerSecond float64) {
	// --- Retrieve the previous captured metric
	previous, exists := r.samples[snapshot.Name]
	r.rememberSample(snapshot) // Update last recorded

	if !exists {
		return 0, 0
	}

	elapsed := snapshot.CapturedAt.Sub(
		previous.capturedAt,
	).Seconds()

	if elapsed <= 0 {
		return 0, 0
	}

	if snapshot.PacketsGenerated >= previous.packetsGenerated {
		generatedPerSecond = float64(snapshot.PacketsGenerated-previous.packetsGenerated) / elapsed
	}

	if snapshot.PacketsSent >= previous.packetsSent {
		sentPerSecond = float64(snapshot.PacketsSent-previous.packetsSent) / elapsed
	}

	return generatedPerSecond, sentPerSecond
}

// observedLossPercent measures cumulative intentional drops, not network loss.
func observedLossPercent(snapshot engine.StreamSnapshot) float64 {
	if snapshot.PacketsGenerated == 0 {
		return 0
	}

	return float64(snapshot.PacketsDropped) / float64(snapshot.PacketsGenerated) * 100
}
