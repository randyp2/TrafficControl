package engine

import (
	"math/rand"
	"time"
)

// sampleDelay picks a random duration between [latency - jitter, latency + jitter]
// Callers must validate 0 <= jitter <= latency and that latency+jitter fits a duration.
func sampleDelay(
	rng *rand.Rand,
	latency time.Duration,
	jitter time.Duration,
) time.Duration {
	if jitter == 0 {
		return latency
	}
	width := int64(jitter)*2 + 1
	offset := time.Duration(rng.Int63n(width)) - jitter

	return latency + offset
}
