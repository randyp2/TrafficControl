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
	color   bool
	header  bool
}

func newConsoleReporter(output io.Writer) *consoleReporter {
	return &consoleReporter{
		output:  output,
		samples: make(map[string]rateSample),
		color:   consoleColor(output),
	}
}

// Report writes one complete update so other streams cannot split its lines.
func (r *consoleReporter) Report(update engine.StreamUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := update.Snapshot
	var output strings.Builder
	elapsed := max(snapshot.CapturedAt.Sub(snapshot.StartedAt), 0).Seconds()
	if update.Kind != engine.UpdateSnapshot {
		color := colorCyan
		switch update.Kind {
		case engine.UpdateStarted, engine.UpdateResumed, engine.UpdateStopped:
			color = colorGreen
		case engine.UpdatePaused, engine.UpdateCanceled, engine.UpdateLossChanged:
			color = colorYellow
		case engine.UpdateFailed:
			color = colorRed
		}
		label := fmt.Sprintf("[%s: %s]", strings.ToUpper(string(update.Kind)), snapshot.Name)
		fmt.Fprintf(&output, "%7.2fs %s ", elapsed, colorize(label, color, r.color))
	}

	switch update.Kind {
	case engine.UpdateStarted:
		r.rememberSample(update.Snapshot)

		fmt.Fprintf(
			&output,
			"target=%d packets/s\n",
			snapshot.TargetRate,
		)

	case engine.UpdateSnapshot:
		generatedRate, sentRate := r.actualRates(snapshot)
		if !r.header {
			output.WriteString("\nRates: packets/s | DROPPED: total intentional drops | LOSS: current setting\n")
			columns := fmt.Sprintf("%8s %-16s %-8s %10s %10s %10s %9s %7s %9s\n",
				"TIME", "STREAM", "STATUS", "TARGET/s", "GEN/s", "SENT/s", "DROPPED", "LOSS %", "LATENCY")
			output.WriteString(colorize(columns, colorCyan, r.color))
			r.header = true
		}

		fmt.Fprintf(
			&output,
			"%7.2fs %-16s %-8s %10d %10.1f %10.1f %9d %7.2f %9s\n",
			elapsed,
			snapshot.Name,
			snapshot.Status,
			snapshot.TargetRate,
			generatedRate,
			sentRate,
			snapshot.PacketsDropped,
			snapshot.LossPercent,
			snapshot.Latency,
		)

	case engine.UpdateRateChanged:
		fmt.Fprintf(
			&output,
			"target=%d packets/s status=%s\n",
			snapshot.TargetRate,
			snapshot.Status,
		)

	case engine.UpdateLatencyChanged:
		fmt.Fprintf(
			&output,
			"latency=%s status=%s\n",
			snapshot.Latency,
			snapshot.Status,
		)

	case engine.UpdateLossChanged:
		fmt.Fprintf(
			&output,
			"loss=%.2f%% status=%s\n",
			snapshot.LossPercent,
			snapshot.Status,
		)

	case engine.UpdatePaused:
		fmt.Fprintf(
			&output,
			"sent=%d bytes=%d\n",
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateResumed:
		fmt.Fprintf(
			&output,
			"target=%d packets/s\n",
			snapshot.TargetRate,
		)

	case engine.UpdateStopped, engine.UpdateCanceled:
		fmt.Fprintf(
			&output,
			"sent=%d bytes=%d\n",
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)
		r.writeSummary(&output, update)

	case engine.UpdateFailed:
		fmt.Fprintf(
			&output,
			"error=%q sent=%d bytes=%d\n",
			snapshot.LastError,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)
		r.writeSummary(&output, update)

	default:
		fmt.Fprintf(
			&output,
			"status=%s\n",
			snapshot.Status,
		)
	}
	io.WriteString(r.output, output.String())
}

func (r *consoleReporter) writeSummary(output io.Writer, update engine.StreamUpdate) {
	snapshot := update.Snapshot
	duration := snapshot.CapturedAt.Sub(snapshot.StartedAt)
	duration = max(duration, 0)

	var averageRate, averageGeneratedRate float64
	if duration > 0 {
		averageRate = float64(snapshot.PacketsSent) / duration.Seconds()
		averageGeneratedRate = float64(snapshot.PacketsGenerated) / duration.Seconds()
	}

	fmt.Fprintf(
		output,
		"\n%s  duration=%.3fs reason=%s\n"+
			"  Generated: %-12d Sent: %-12d Dropped: %d\n"+
			"  Average generation: %.1f/s | Average sending: %.1f/s\n"+
			"  Target: %d/s | Latency: %s | Configured loss: %.2f%%\n"+
			"  Intentional drops across entire run: %.2f%% | Bytes sent: %d\n\n",
		colorize(fmt.Sprintf("[SUMMARY: %s]", snapshot.Name), colorCyan, r.color),
		duration.Seconds(),
		update.Kind,
		snapshot.PacketsGenerated,
		snapshot.PacketsSent,
		snapshot.PacketsDropped,
		averageGeneratedRate,
		averageRate,
		snapshot.TargetRate,
		snapshot.Latency,
		snapshot.LossPercent,
		observedLossPercent(snapshot),
		snapshot.BytesSent,
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
