package engine

import (
	"container/heap"
	"time"
)

// delayedPacket is a packet that will be sent at a later time
type delayedPacket struct {
	payload   []byte
	releaseAt time.Time
	sequence  uint64
}

/* ======== HEAP INTERFACE IMPLEMENTATIONS ======== */
// define heap to store our delayed packets
type packetHeap []delayedPacket

func (h packetHeap) Len() int {
	return len(h)
}

func (h packetHeap) Less(i int, j int) bool {
	if h[i].releaseAt.Equal(h[j].releaseAt) {
		return h[i].sequence < h[j].sequence
	}

	return h[i].releaseAt.Before(h[j].releaseAt)
}

func (h packetHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *packetHeap) Push(value any) {
	packet := value.(delayedPacket)
	*h = append(*h, packet)
}

func (h *packetHeap) Pop() any {
	old := *h
	last := len(old) - 1

	packet := old[last]

	old[last] = delayedPacket{}
	*h = old[:last]

	return packet
}

/* ======== DELAY QUEUE IMPLEMENTATION ======== */
type delayQueue struct {
	packets      packetHeap
	nextSequence uint64
}

func newDelayQueue() *delayQueue {
	queue := &delayQueue{}
	heap.Init(&queue.packets)

	return queue
}

// Enqueue pushes a delayedPacket on to the internal heap
func (q *delayQueue) Enqueue(
	payload []byte,
	releaseAt time.Time,
) {
	packet := delayedPacket{
		payload:   payload,
		releaseAt: releaseAt,
		sequence:  q.nextSequence,
	}

	q.nextSequence++

	heap.Push(&q.packets, packet)
}

// Len returns the length of the internal heap of the delayedQueue
func (q *delayQueue) Len() int {
	return q.packets.Len()
}

// Peek returns the packet w/ earliest return time w/o removing it
func (q *delayQueue) Peek() (delayedPacket, bool) {
	if q.Len() == 0 {
		return delayedPacket{}, false
	}

	return q.packets[0], true
}

// PopDue removes and returns the earliest packet due when its release time has arrived
func (q *delayQueue) PopDue(now time.Time) (delayedPacket, bool) {
	packet, exists := q.Peek()

	if !exists {
		return delayedPacket{}, false
	}

	if packet.releaseAt.After(now) {
		return delayedPacket{}, false
	}

	packet = heap.Pop(&q.packets).(delayedPacket)
	return packet, true
}
