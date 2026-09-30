package main

import (
	"fmt"
	"io"
	"sync"

	"github.com/randyp2/trafficcontrol/internal/engine"
)

// consoleReporter implements report and holds mutex lock preventing jumbled output
type consoleReporter struct {
	mu     sync.Mutex
	output io.Writer
}

func newConsoleReporter(output io.Writer) *consoleReporter {
	return &consoleReporter{
		output: output,
	}
}

func (r *consoleReporter) Report(update engine.StreamUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := update.Snapshot

	switch update.Kind {
	case engine.UpdateStarted:
		fmt.Fprintf(
			r.output,
			"[%s] started rate=%d pkt/s\n",
			snapshot.Name,
			snapshot.TargetRate,
		)

	case engine.UpdateSnapshot:
		fmt.Fprintf(
			r.output,
			"[%s] status=%s rate=%d pkt/s packets=%d bytes=%d\n",
			snapshot.Name,
			snapshot.Status,
			snapshot.TargetRate,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateRateChanged:
		fmt.Fprintf(
			r.output,
			"[%s] rate changed rate=%d pkt/s status=%s\n",
			snapshot.Name,
			snapshot.TargetRate,
			snapshot.Status,
		)

	case engine.UpdatePaused:
		fmt.Fprintf(
			r.output,
			"[%s] paused packets=%d bytes=%d\n",
			snapshot.Name,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateResumed:
		fmt.Fprintf(
			r.output,
			"[%s] resumed rate=%d pkt/s\n",
			snapshot.Name,
			snapshot.TargetRate,
		)

	case engine.UpdateStopped:
		fmt.Fprintf(
			r.output,
			"[%s] stopped packets=%d bytes=%d\n",
			snapshot.Name,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateCanceled:
		fmt.Fprintf(
			r.output,
			"[%s] canceled packets=%d bytes=%d\n",
			snapshot.Name,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	case engine.UpdateFailed:
		fmt.Fprintf(
			r.output,
			"[%s] failed error=%q packets=%d bytes=%d\n",
			snapshot.Name,
			snapshot.LastError,
			snapshot.PacketsSent,
			snapshot.BytesSent,
		)

	default:
		fmt.Fprintf(
			r.output,
			"[%s] update=%s status=%s\n",
			snapshot.Name,
			update.Kind,
			snapshot.Status,
		)
	}
}
