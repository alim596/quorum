// Package cluster is the single runtime that drives N raft nodes over the
// virtual network — used unchanged by both the deterministic simulation
// tests and the live chaos lab (the lab just calls Step on a wall clock).
// One goroutine, one seed, total determinism.
package cluster

import (
	"fmt"
	"math/rand"

	"github.com/alim596/quorum/internal/raft"
	"github.com/alim596/quorum/internal/vnet"
)

// Event is a notable cluster happening, for the lab console and for tests.
type Event struct {
	Tick uint64 `json:"tick"`
	Node int    `json:"node"`
	Kind string `json:"kind"` // elected | stepped-down | crashed | restarted | partitioned | healed | proposal
	Text string `json:"text"`
	Term uint64 `json:"term,omitempty"`
}

const maxEvents = 400

type Cluster struct {
	IDs   []int
	nodes map[int]*raft.Node
	Net   *vnet.Network
	seed  int64

	// "disk": survives crashes, feeds restarts
	hard map[int]raft.HardState
	logs map[int][]raft.Entry

	// state machine hooks
	apply     func(id int, e raft.Entry)
	onRestart func(id int)

	// observability
	role   map[int]raft.Role
	events []Event

	// invariant recording (tests)
	Record  bool
	Applied map[int][]raft.Entry
	// leaders observed per term, ever (election safety evidence)
	LeadersByTerm map[uint64]map[int]bool
}

func New(seed int64, n int, apply func(int, raft.Entry), onRestart func(int)) *Cluster {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	c := &Cluster{
		IDs: ids, nodes: map[int]*raft.Node{}, seed: seed,
		Net:  vnet.New(seed, ids),
		hard: map[int]raft.HardState{}, logs: map[int][]raft.Entry{},
		apply: apply, onRestart: onRestart,
		role:          map[int]raft.Role{},
		Applied:       map[int][]raft.Entry{},
		LeadersByTerm: map[uint64]map[int]bool{},
	}
	for _, id := range ids {
		c.bootNode(id)
	}
	return c
}

func (c *Cluster) nodeConfig(id int) raft.Config {
	return raft.Config{
		ID: id, Peers: c.IDs,
		// Deterministic per-node RNG derived from the cluster seed and the
		// node's restart count folded into the current tick.
		Rand:               rand.New(rand.NewSource(c.seed*1_000_003 + int64(id)*7919 + int64(c.Net.Now()))),
		ElectionTimeoutMin: 14, ElectionTimeoutMax: 28, HeartbeatInterval: 4,
	}
}

func (c *Cluster) bootNode(id int) {
	n, err := raft.NewNode(c.nodeConfig(id), c.hard[id], append([]raft.Entry(nil), c.logs[id]...))
	if err != nil {
		panic(err) // config is static; an error here is a programming bug
	}
	c.nodes[id] = n
	c.role[id] = raft.Follower
}

// Step advances the whole cluster one tick: deliver due messages, tick every
// live node, then drain and act on each node's Ready.
func (c *Cluster) Step() {
	for _, m := range c.Net.Tick() {
		if n := c.nodes[m.To]; n != nil && !c.Net.IsDown(m.To) {
			n.Step(m)
		}
	}
	for _, id := range c.IDs {
		if n := c.nodes[id]; n != nil && !c.Net.IsDown(id) {
			n.Tick()
		}
	}
	for _, id := range c.IDs {
		n := c.nodes[id]
		if n == nil || c.Net.IsDown(id) {
			continue
		}
		rd := n.Ready()

		// persist BEFORE sending (§5.1). The "disk" is a map; a real
		// implementation would fsync a WAL segment here.
		if len(rd.AppendEntries) > 0 {
			c.logs[id] = append([]raft.Entry(nil), n.Entries()...)
		}
		if rd.HardState != nil {
			c.hard[id] = *rd.HardState
		}
		for _, m := range rd.Messages {
			c.Net.Send(m)
		}
		for _, e := range rd.CommittedSlice {
			if c.apply != nil {
				c.apply(id, e)
			}
			if c.Record {
				c.Applied[id] = append(c.Applied[id], e)
			}
		}
		if rd.SoftState != nil {
			c.noteSoftState(id, rd.SoftState)
		}
	}
}

func (c *Cluster) noteSoftState(id int, ss *raft.SoftState) {
	prev := c.role[id]
	c.role[id] = ss.Role
	if ss.Role == raft.Leader {
		if c.LeadersByTerm[ss.Term] == nil {
			c.LeadersByTerm[ss.Term] = map[int]bool{}
		}
		c.LeadersByTerm[ss.Term][id] = true
		if prev != raft.Leader {
			c.event(id, "elected", ss.Term, fmt.Sprintf("node %d elected leader (term %d)", id, ss.Term))
		}
	} else if prev == raft.Leader {
		c.event(id, "stepped-down", ss.Term, fmt.Sprintf("node %d stepped down (term %d)", id, ss.Term))
	}
}

func (c *Cluster) event(node int, kind string, term uint64, text string) {
	c.events = append(c.events, Event{Tick: c.Net.Now(), Node: node, Kind: kind, Term: term, Text: text})
	if len(c.events) > maxEvents {
		c.events = c.events[len(c.events)-maxEvents:]
	}
}

// ---- chaos controls ----

func (c *Cluster) Crash(id int) {
	if c.nodes[id] == nil || c.Net.IsDown(id) {
		return
	}
	c.Net.SetDown(id, true)
	c.nodes[id] = nil // volatile state gone; disk (c.hard/c.logs) survives
	c.role[id] = raft.Follower
	c.event(id, "crashed", 0, fmt.Sprintf("node %d crashed (volatile state lost)", id))
}

func (c *Cluster) Restart(id int) {
	if !c.Net.IsDown(id) {
		return
	}
	c.Net.SetDown(id, false)
	if c.onRestart != nil {
		c.onRestart(id) // state machine rebuilds by replaying the log
	}
	if c.Record {
		c.Applied[id] = nil
	}
	c.bootNode(id)
	c.event(id, "restarted", 0, fmt.Sprintf("node %d restarted from disk (term %d, %d log entries)", id, c.hard[id].Term, len(c.logs[id])))
}

func (c *Cluster) Partition(groups [][]int) {
	c.Net.SetPartition(groups)
	c.event(0, "partitioned", 0, fmt.Sprintf("network partitioned: %v", groups))
}

func (c *Cluster) Heal() {
	c.Net.Heal()
	c.event(0, "healed", 0, "network healed")
}

// ---- client path ----

// Propose submits a command to whichever live node currently claims
// leadership. Returns the node and log index, or an error if no leader.
func (c *Cluster) Propose(data []byte) (leaderID int, index uint64, err error) {
	for _, id := range c.IDs {
		n := c.nodes[id]
		if n == nil || c.Net.IsDown(id) {
			continue
		}
		if idx, _, ok := n.Propose(data); ok {
			return id, idx, nil
		}
	}
	return 0, 0, fmt.Errorf("no leader available")
}

// ---- observability ----

func (c *Cluster) Statuses() []raft.Status {
	out := make([]raft.Status, 0, len(c.IDs))
	for _, id := range c.IDs {
		if n := c.nodes[id]; n != nil {
			s := n.Status()
			out = append(out, s)
		} else {
			out = append(out, raft.Status{ID: id, Role: "down", VotedFor: -1, Leader: -1})
		}
	}
	return out
}

func (c *Cluster) Events() []Event { return append([]Event(nil), c.events...) }

func (c *Cluster) Node(id int) *raft.Node { return c.nodes[id] }

func (c *Cluster) AliveQuorum() bool {
	alive := 0
	for _, id := range c.IDs {
		if !c.Net.IsDown(id) {
			alive++
		}
	}
	return alive >= len(c.IDs)/2+1
}
