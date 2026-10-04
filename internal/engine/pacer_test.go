package engine

import (
	"testing"
	"time"
)

func TestIntervalForRate(t *testing.T) {
	tests := []struct {
		name    string
		rate    int
		want    time.Duration
		wantErr bool
	}{
		{
			name: "valid rate",
			rate: 1000,
			want: time.Millisecond,
		},
		{
			name:    "zero rate",
			rate:    0,
			wantErr: true,
		},
		{
			name:    "negative rate",
			rate:    -1,
			wantErr: true,
		},
		{
			name:    "rate exceeds timer precision",
			rate:    int(time.Second) + 1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := intervalForRate(tt.rate)
			if (err != nil) != tt.wantErr {
				t.Fatalf("intervalForRate() error = %v, wantErr %t", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("intervalForRate() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBoundedPacerPacketsDue(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	pacer := newTestPacer(t, 1000, startedAt)

	if got := pacer.packetsDue(startedAt.Add(500 * time.Microsecond)); got != 0 {
		t.Fatalf("packetsDue() before deadline = %d, want 0", got)
	}

	if got := pacer.packetsDue(startedAt.Add(time.Millisecond)); got != 1 {
		t.Fatalf("packetsDue() at first deadline = %d, want 1", got)
	}
}

func TestBoundedPacerCatchesUpWithinLimit(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	pacer := newTestPacer(t, 1000, startedAt)

	if got := pacer.packetsDue(startedAt.Add(4 * time.Millisecond)); got != 4 {
		t.Fatalf("packetsDue() = %d, want 4", got)
	}
}

func TestBoundedPacerCapsCatchUpAndDiscardsOlderDebt(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	pacer := newTestPacer(t, 1000, startedAt)
	wokeAt := startedAt.Add(20 * time.Millisecond)

	if got := pacer.packetsDue(wokeAt); got != 5 {
		t.Fatalf("packetsDue() = %d, want 5", got)
	}

	wantNext := wokeAt.Add(time.Millisecond)
	if !pacer.next.Equal(wantNext) {
		t.Fatalf("next deadline = %s, want %s", pacer.next, wantNext)
	}
}

func TestBoundedPacerMaximumBatchScalesWithRate(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		rate int
		want int
	}{
		{
			name: "minimum one packet",
			rate: 10,
			want: 1,
		},
		{
			name: "one thousand packets per second",
			rate: 1000,
			want: 5,
		},
		{
			name: "one hundred fifty thousand packets per second",
			rate: 150000,
			want: 750,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pacer := newTestPacer(t, tt.rate, startedAt)
			if got := pacer.maximumBatch(); got != tt.want {
				t.Fatalf("maximumBatch() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBoundedPacerPauseResumeClearsDebt(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	pacer := newTestPacer(t, 1000, startedAt)

	pacer.Pause()
	if got := pacer.packetsDue(startedAt.Add(time.Second)); got != 0 {
		t.Fatalf("packetsDue() while paused = %d, want 0", got)
	}

	resumedAt := startedAt.Add(time.Second)
	pacer.Resume(resumedAt)

	if got := pacer.packetsDue(resumedAt); got != 0 {
		t.Fatalf("packetsDue() at resume = %d, want 0", got)
	}
	if got := pacer.packetsDue(resumedAt.Add(time.Millisecond)); got != 1 {
		t.Fatalf("packetsDue() after resume = %d, want 1", got)
	}
}

func TestBoundedPacerSetRateWhilePaused(t *testing.T) {
	startedAt := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	pacer := newTestPacer(t, 1000, startedAt)
	pacer.Pause()

	rateChangedAt := startedAt.Add(time.Second)
	if err := pacer.SetRate(2000, rateChangedAt); err != nil {
		t.Fatalf("SetRate() error = %v", err)
	}

	if pacer.running {
		t.Fatal("SetRate() resumed a paused pacer")
	}
	if pacer.interval != 500*time.Microsecond {
		t.Fatalf("interval = %s, want %s", pacer.interval, 500*time.Microsecond)
	}

	resumedAt := rateChangedAt.Add(time.Second)
	pacer.Resume(resumedAt)
	if got := pacer.packetsDue(resumedAt.Add(500 * time.Microsecond)); got != 1 {
		t.Fatalf("packetsDue() after rate change and resume = %d, want 1", got)
	}
}

func newTestPacer(
	t *testing.T,
	rate int,
	startedAt time.Time,
) *boundedPacer {
	t.Helper()

	pacer, err := newBoundedPacer(rate, startedAt)
	if err != nil {
		t.Fatalf("newBoundedPacer() error = %v", err)
	}
	t.Cleanup(pacer.Stop)

	return pacer
}
