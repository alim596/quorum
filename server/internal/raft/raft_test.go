package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// net is a perfect in-memory network for unit tests: every Ready is drained
// and delivered instantly (unless a node is isolated).
type net struct {
	t        *testing.T
	nodes    map[int]*Node
	isolated map[int]bool
	applied  map[int][]Entry
}

func newNet(t *testing.T, n int) *net {
	nn := &net{
		t: t, nodes: map[int]*Node{}, isolated: map[int]bool{},
		applied: map[int][]Entry{},
	}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i + 1
	}
	for _, id := range peers {
		node, err := NewNode(Config{
			ID: id, Peers: peers,
			Rand:               rand.New(rand.NewSource(int64(id) * 7919)),
			ElectionTimeoutMin: 10, ElectionTimeoutMax: 20, HeartbeatInterval: 3,
		}, HardState{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		nn.nodes[id] = node
	}
	return nn
}

// settle drains all pending messages until quiescent.
func (nn *net) settle() {
	for i := 0; i < 10000; i++ {
		moved := false
		for id, node := range nn.nodes {
			rd := node.Ready()
			nn.applied[id] = append(nn.applied[id], rd.CommittedSlice...)
			for _, m := range rd.Messages {
				if nn.isolated[m.From] || nn.isolated[m.To] {
					continue
				}
				nn.nodes[m.To].Step(m)
				moved = true
			}
		}
		if !moved {
			return
		}
	}
	nn.t.Fatal("network did not quiesce")
}

func (nn *net) tickAll(k int) {
	for i := 0; i < k; i++ {
		for id, node := range nn.nodes {
			if !nn.isolated[id] {
				node.Tick()
			}
		}
		nn.settle()
	}
}

func (nn *net) leader() *Node {
	var lead *Node
	for _, node := range nn.nodes {
		if node.role == Leader {
			if lead != nil && lead.term == node.term {
				nn.t.Fatalf("two leaders in term %d", node.term)
			}
			if lead == nil || node.term > lead.term {
				lead = node
			}
		}
	}
	return lead
}

func (nn *net) mustLeader() *Node {
	lead := nn.leader()
	if lead == nil {
		nn.t.Fatal("no leader elected")
	}
	return lead
}

func TestLeaderElection(t *testing.T) {
	nn := newNet(t, 3)
	nn.tickAll(25)
	lead := nn.mustLeader()
	if lead.term == 0 {
		t.Fatal("leader with term 0")
	}
	// all connected nodes agree on the leader
	for _, node := range nn.nodes {
		if node.leader != lead.cfg.ID {
			t.Fatalf("node %d thinks leader is %d, want %d", node.cfg.ID, node.leader, lead.cfg.ID)
		}
	}
}

func TestReplicationAndCommit(t *testing.T) {
	nn := newNet(t, 5)
	nn.tickAll(25)
	lead := nn.mustLeader()

	idx, _, ok := lead.Propose([]byte("x=1"))
	if !ok {
		t.Fatal("leader refused proposal")
	}
	nn.settle()

	for id, node := range nn.nodes {
		if node.CommitIndex() < idx {
			t.Fatalf("node %d commit %d < proposed %d", id, node.CommitIndex(), idx)
		}
	}
	// every node applied the same entry data at that index
	for id, applied := range nn.applied {
		found := false
		for _, e := range applied {
			if e.Index == idx && string(e.Data) == "x=1" {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %d never applied the proposal", id)
		}
	}
}

func TestFollowerProposalRefused(t *testing.T) {
	nn := newNet(t, 3)
	nn.tickAll(25)
	lead := nn.mustLeader()
	for _, node := range nn.nodes {
		if node != lead {
			if _, _, ok := node.Propose([]byte("nope")); ok {
				t.Fatal("follower accepted a proposal")
			}
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	nn := newNet(t, 5)
	nn.tickAll(25)
	old := nn.mustLeader()
	old.Propose([]byte("a"))
	nn.settle()

	// isolate the leader; the rest must elect a successor
	nn.isolated[old.cfg.ID] = true
	nn.tickAll(30)
	newLead := nn.mustLeader()
	if newLead.cfg.ID == old.cfg.ID {
		t.Fatal("isolated leader still counted as leader")
	}
	if newLead.term <= old.term {
		t.Fatalf("successor term %d not beyond old term %d", newLead.term, old.term)
	}
	// leader completeness: the committed entry survives the failover
	found := false
	for _, e := range newLead.Entries() {
		if string(e.Data) == "a" {
			found = true
		}
	}
	if !found {
		t.Fatal("committed entry lost across failover")
	}

	// old leader rejoins and converges
	nn.isolated[old.cfg.ID] = false
	nn.tickAll(10)
	if old.role == Leader {
		t.Fatal("stale leader did not step down after rejoining")
	}
}

func TestLogConflictResolution(t *testing.T) {
	nn := newNet(t, 5)
	nn.tickAll(25)
	lead := nn.mustLeader()

	// leader accepts entries while isolated -> they must be overwritten
	nn.isolated[lead.cfg.ID] = true
	lead.Propose([]byte("doomed-1"))
	lead.Propose([]byte("doomed-2"))

	nn.tickAll(30)
	newLead := nn.mustLeader()
	winIdx, _, _ := newLead.Propose([]byte("winner"))
	nn.settle()

	nn.isolated[lead.cfg.ID] = false
	nn.tickAll(10)

	// stale entries replaced by the new leader's log on the old leader
	var got []string
	for _, e := range nn.nodes[lead.cfg.ID].Entries() {
		if len(e.Data) > 0 {
			got = append(got, string(e.Data))
		}
	}
	for _, s := range got {
		if s == "doomed-1" || s == "doomed-2" {
			t.Fatalf("uncommitted stale entries survived: %v", got)
		}
	}
	if nn.nodes[lead.cfg.ID].CommitIndex() < winIdx {
		t.Fatal("rejoined node did not catch up to winner entry")
	}
}

func TestPersistRestore(t *testing.T) {
	nn := newNet(t, 3)
	nn.tickAll(25)
	lead := nn.mustLeader()
	lead.Propose([]byte("durable"))
	nn.settle()

	// capture a follower's persistent state, "restart" it, and check that
	// the restored node keeps its term/vote/log
	var f *Node
	for _, node := range nn.nodes {
		if node.role == Follower {
			f = node
			break
		}
	}
	hs := HardState{Term: f.term, VotedFor: f.votedFor, Commit: f.CommitIndex()}
	entries := append([]Entry(nil), f.Entries()...)

	restored, err := NewNode(Config{
		ID: f.cfg.ID, Peers: f.cfg.Peers,
		Rand:               rand.New(rand.NewSource(1)),
		ElectionTimeoutMin: 10, ElectionTimeoutMax: 20, HeartbeatInterval: 3,
	}, hs, entries)
	if err != nil {
		t.Fatal(err)
	}
	if restored.term != f.term || restored.votedFor != f.votedFor {
		t.Fatal("hard state lost on restore")
	}
	if restored.log.lastIndex() != f.log.lastIndex() {
		t.Fatal("log lost on restore")
	}
}

func TestElectionSafetyManySeeds(t *testing.T) {
	// crude pre-sim check: many seeds, isolated elections, never 2 leaders/term
	for seed := int64(0); seed < 30; seed++ {
		nn := newNet(t, 5)
		for id, node := range nn.nodes {
			node.cfg.Rand = rand.New(rand.NewSource(seed*100 + int64(id)))
		}
		nn.tickAll(60)
		leadersByTerm := map[uint64][]int{}
		for id, node := range nn.nodes {
			if node.role == Leader {
				leadersByTerm[node.term] = append(leadersByTerm[node.term], id)
			}
		}
		for term, ids := range leadersByTerm {
			if len(ids) > 1 {
				t.Fatalf("seed %d: term %d has leaders %v", seed, term, ids)
			}
		}
		_ = fmt.Sprint(seed)
	}
}
