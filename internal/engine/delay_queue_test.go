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

func TestDelayQueuePopDue(t *testing.T) {
	now := time.Date(
		2026,
		time.October,
		10,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	queue := newDelayQueue()
	queue.Enqueue([]byte("second"), now.Add(2*time.Millisecond))
	queue.Enqueue([]byte("first"), now.Add(time.Millisecond))

	if _, ready := queue.PopDue(now); ready {
		t.Fatal("PopDue() returned a packet before its release time")
	}

	first, ready := queue.PopDue(now.Add(time.Millisecond))
	if !ready {
		t.Fatal("PopDue() did not return the first due packet")
	}
	if got := string(first.payload); got != "first" {
		t.Fatalf("first payload = %q, want %q", got, "first")
	}

	second, ready := queue.PopDue(now.Add(2 * time.Millisecond))
	if !ready {
		t.Fatal("PopDue() did not return the second due packet")
	}
	if got := string(second.payload); got != "second" {
		t.Fatalf("second payload = %q, want %q", got, "second")
	}

	if queue.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", queue.Len())
	}
}
