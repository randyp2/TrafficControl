package engine

import (
	"container/heap"
	"testing"
	"time"
)

func TestPacketHeapOrderByReleaseTimeThenSequence(t *testing.T) {
	startedAt := time.Date(
		2026,
		time.October,
		8,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	packets := &packetHeap{}
	heap.Init(packets)

	heap.Push(packets, delayedPacket{
		releaseAt: startedAt.Add(2 * time.Millisecond),
		sequence:  2,
	})

	// Same time different sequencing
	heap.Push(packets, delayedPacket{
		releaseAt: startedAt.Add(time.Millisecond),
		sequence:  1,
	})

	heap.Push(packets, delayedPacket{
		releaseAt: startedAt.Add(time.Millisecond),
		sequence:  0,
	})

	wantSequences := []uint64{0, 1, 2}

	for _, want := range wantSequences {
		packet := heap.Pop(packets).(delayedPacket)

		if packet.sequence != want {
			t.Fatalf(
				"heap.Pop() sequence = %d, want %d",
				packet.sequence,
				want,
			)
		}
	}
}
