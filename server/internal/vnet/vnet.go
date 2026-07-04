// Package vnet is a virtual network with dials for everything that goes
// wrong in real ones: latency, jitter, packet loss, partitions, and dead
// hosts. It is fully deterministic given a seed — the same chaos replays
// identically, which is what makes simulation failures debuggable.
package vnet

import (
	"container/heap"
	"math/rand"

	"github.com/alim596/quorum/internal/raft"
)

// Inflight is a message in transit, exposed for the lab's animation.
type Inflight struct {
	Msg       raft.Message `json:"msg"`
	SentAt    uint64       `json:"sentAt"`
	DeliverAt uint64       `json:"deliverAt"`
}

type item struct {
	Inflight
	seq uint64 // FIFO tiebreak for equal delivery ticks (determinism)
}

type pq []item

func (p pq) Len() int { return len(p) }
func (p pq) Less(i, j int) bool {
	if p[i].DeliverAt != p[j].DeliverAt {
		return p[i].DeliverAt < p[j].DeliverAt
	}
	return p[i].seq < p[j].seq
}
func (p pq) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x any)        { *p = append(*p, x.(item)) }
func (p *pq) Pop() any          { old := *p; n := len(old); it := old[n-1]; *p = old[:n-1]; return it }

type Network struct {
	rng   *rand.Rand
	now   uint64
	seq   uint64
	queue pq

	group      map[int]int // partition group per node; same group = connected
	down       map[int]bool
	lossProb   float64
	latencyMin uint64
	latencyMax uint64

	Dropped uint64 // counters for observability
	Sent    uint64
}

func New(seed int64, nodeIDs []int) *Network {
	n := &Network{
		rng:   rand.New(rand.NewSource(seed)),
		group: make(map[int]int),
		down:  make(map[int]bool),
		latencyMin: 1, latencyMax: 3,
	}
	for _, id := range nodeIDs {
		n.group[id] = 0
	}
	return n
}

func (n *Network) Now() uint64 { return n.now }

// Send routes a message, applying loss, partitions, and dead-host rules.
// Dropped messages vanish silently — exactly like UDP, and exactly what
// Raft is designed to survive.
func (n *Network) Send(m raft.Message) {
	n.Sent++
	if n.down[m.From] || n.down[m.To] ||
		n.group[m.From] != n.group[m.To] ||
		(n.lossProb > 0 && n.rng.Float64() < n.lossProb) {
		n.Dropped++
		return
	}
	span := n.latencyMax - n.latencyMin
	lat := n.latencyMin
	if span > 0 {
		lat += uint64(n.rng.Intn(int(span + 1)))
	}
	n.seq++
	heap.Push(&n.queue, item{
		Inflight: Inflight{Msg: m, SentAt: n.now, DeliverAt: n.now + lat},
		seq:      n.seq,
	})
}

// Tick advances virtual time one unit and returns the messages due now.
// Messages addressed to nodes that died in transit are dropped on arrival.
func (n *Network) Tick() []raft.Message {
	n.now++
	var due []raft.Message
	for n.queue.Len() > 0 && n.queue[0].DeliverAt <= n.now {
		it := heap.Pop(&n.queue).(item)
		if n.down[it.Msg.To] || n.group[it.Msg.From] != n.group[it.Msg.To] {
			n.Dropped++
			continue
		}
		due = append(due, it.Msg)
	}
	return due
}

// ---- chaos dials ----

// SetPartition splits the cluster into isolated groups. Nodes absent from
// every group land in their own singleton (fully isolated).
func (n *Network) SetPartition(groups [][]int) {
	next := len(groups) + 1
	for id := range n.group {
		placed := false
		for gi, g := range groups {
			for _, member := range g {
				if member == id {
					n.group[id] = gi
					placed = true
				}
			}
		}
		if !placed {
			n.group[id] = next
			next++
		}
	}
}

func (n *Network) Heal() {
	for id := range n.group {
		n.group[id] = 0
	}
}

func (n *Network) SetLoss(p float64) {
	if p < 0 {
		p = 0
	}
	if p > 0.9 {
		p = 0.9
	}
	n.lossProb = p
}

func (n *Network) SetLatency(min, max uint64) {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	n.latencyMin, n.latencyMax = min, max
}

func (n *Network) SetDown(id int, down bool) { n.down[id] = down }

func (n *Network) IsDown(id int) bool { return n.down[id] }

// Inflight lists undelivered messages (for the lab animation).
func (n *Network) Inflight() []Inflight {
	out := make([]Inflight, 0, n.queue.Len())
	for _, it := range n.queue {
		out = append(out, it.Inflight)
	}
	return out
}

// Groups exposes the current partition map (for the lab).
func (n *Network) Groups() map[int]int {
	out := make(map[int]int, len(n.group))
	for k, v := range n.group {
		out[k] = v
	}
	return out
}

// Loss and latency accessors for the lab UI.
func (n *Network) Loss() float64            { return n.lossProb }
func (n *Network) Latency() (uint64, uint64) { return n.latencyMin, n.latencyMax }
