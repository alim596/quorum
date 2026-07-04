package raft

import (
	"fmt"
	"math/rand"
)

// Config for one node. Timeouts are in ticks; the host decides how long a
// tick is (the live cluster uses ~20ms, the simulator uses "whatever").
type Config struct {
	ID       int
	Peers    []int // all node IDs including self
	Rand     *rand.Rand
	ElectionTimeoutMin int // ticks; randomized per election in [min, max)
	ElectionTimeoutMax int
	HeartbeatInterval  int
}

func (c Config) validate() error {
	if c.Rand == nil {
		return fmt.Errorf("raft: Config.Rand is required (determinism is the point)")
	}
	if c.ElectionTimeoutMin <= c.HeartbeatInterval {
		return fmt.Errorf("raft: election timeout must exceed heartbeat interval")
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: ElectionTimeoutMax must exceed Min")
	}
	return nil
}

const maxBatch = 64 // max entries per AppendEntries message

// Node is one Raft participant. All methods must be called from a single
// goroutine; the node performs no I/O and holds no timers.
type Node struct {
	cfg  Config
	role Role

	term     uint64
	votedFor int // -1 = none
	leader   int // -1 = unknown
	log      *raftLog

	votes      map[int]bool    // candidate: granted votes
	nextIndex  map[int]uint64  // leader: next entry to send per peer
	matchIndex map[int]uint64  // leader: highest replicated entry per peer

	electionElapsed  int
	electionDeadline int // randomized
	heartbeatElapsed int

	// pending Ready accumulation
	outMsgs   []Message
	toAppend  []Entry
	hardDirty bool
	softDirty bool
}

// NewNode restores a node from persisted state (zero values for a fresh one).
func NewNode(cfg Config, hs HardState, entries []Entry) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if hs.VotedFor == 0 && hs.Term == 0 {
		hs.VotedFor = -1
	}
	n := &Node{
		cfg:      cfg,
		role:     Follower,
		term:     hs.Term,
		votedFor: hs.VotedFor,
		leader:   -1,
		log:      newLog(entries),
	}
	n.log.commit = hs.Commit
	if n.log.commit > n.log.lastIndex() {
		n.log.commit = n.log.lastIndex()
	}
	n.resetElectionTimer()
	n.softDirty = true
	return n, nil
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionDeadline = n.cfg.ElectionTimeoutMin + n.cfg.Rand.Intn(span)
}

// Tick advances logical time by one unit.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatInterval {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionDeadline {
		n.startElection()
	}
}

func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = -1
	n.votes = map[int]bool{n.cfg.ID: true}
	n.hardDirty = true
	n.softDirty = true
	n.resetElectionTimer()

	if len(n.cfg.Peers) == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.send(Message{
			Type: MsgVoteRequest, To: p, Term: n.term,
			LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(),
		})
	}
}

func (n *Node) becomeFollower(term uint64, leader int) {
	if term > n.term {
		n.term = term
		n.votedFor = -1
		n.hardDirty = true
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.softDirty = true
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.heartbeatElapsed = 0
	n.softDirty = true
	n.nextIndex = make(map[int]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[int]uint64, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// Append a no-op entry for the new term (§5.4.2): a leader may only
	// count replicas for entries of its own term, so committing this no-op
	// is what (transitively) commits everything before it.
	n.appendLocal(nil)
	n.broadcastAppend()
}

// Propose asks the leader to replicate data. Returns the assigned index or
// ok=false when this node is not the leader.
func (n *Node) Propose(data []byte) (index uint64, term uint64, ok bool) {
	if n.role != Leader {
		return 0, 0, false
	}
	idx := n.appendLocal(data)
	n.broadcastAppend()
	return idx, n.term, true
}

func (n *Node) appendLocal(data []byte) uint64 {
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Data: data}
	n.log.append(e)
	n.toAppend = append(n.toAppend, e)
	n.matchIndex[n.cfg.ID] = n.log.lastIndex()
	n.maybeCommit()
	return e.Index
}

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to int) {
	next := n.nextIndex[to]
	prev := next - 1
	m := Message{
		Type: MsgAppendRequest, To: to, Term: n.term,
		PrevLogIndex: prev, PrevLogTerm: n.log.term(prev),
		LeaderCommit: n.log.commit,
	}
	last := n.log.lastIndex()
	if next <= last {
		hi := next + maxBatch - 1
		if hi > last {
			hi = last
		}
		m.Entries = n.log.slice(prev, hi)
	}
	n.send(m)
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	n.outMsgs = append(n.outMsgs, m)
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	if m.Term > n.term {
		leader := -1
		if m.Type == MsgAppendRequest {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	}

	switch m.Type {
	case MsgVoteRequest:
		n.handleVoteRequest(m)
	case MsgVoteResponse:
		n.handleVoteResponse(m)
	case MsgAppendRequest:
		n.handleAppendRequest(m)
	case MsgAppendResponse:
		n.handleAppendResponse(m)
	}
}

func (n *Node) handleVoteRequest(m Message) {
	granted := m.Term >= n.term &&
		(n.votedFor == -1 || n.votedFor == m.From) &&
		n.log.upToDate(m.LastLogIndex, m.LastLogTerm)
	if granted {
		n.votedFor = m.From
		n.hardDirty = true
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResponse, To: m.From, Term: n.term, Granted: granted})
}

func (n *Node) handleVoteResponse(m Message) {
	if n.role != Candidate || m.Term < n.term {
		return
	}
	n.votes[m.From] = m.Granted
	granted := 0
	for _, g := range n.votes {
		if g {
			granted++
		}
	}
	if granted >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendRequest(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResponse, To: m.From, Term: n.term, Success: false})
		return
	}
	// valid leader for our term: follow it
	n.role = Follower
	n.leader = m.From
	n.softDirty = true
	n.resetElectionTimer()

	// consistency check (§5.3)
	if m.PrevLogIndex > n.log.lastIndex() {
		n.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: n.term,
			Success: false, RejectHint: n.log.lastIndex(),
		})
		return
	}
	if n.log.term(m.PrevLogIndex) != m.PrevLogTerm {
		hint := m.PrevLogIndex - 1
		n.send(Message{
			Type: MsgAppendResponse, To: m.From, Term: n.term,
			Success: false, RejectHint: hint,
		})
		return
	}

	// append, resolving conflicts idempotently (messages may be stale or
	// reordered by the network — never truncate on a mere duplicate)
	for _, e := range m.Entries {
		switch {
		case e.Index <= n.log.lastIndex() && n.log.term(e.Index) == e.Term:
			continue // already have it
		case e.Index <= n.log.lastIndex():
			n.log.truncateFrom(e.Index)
			fallthrough
		default:
			n.log.append(e)
			n.toAppend = append(n.toAppend, e)
		}
	}

	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > n.log.commit {
		c := m.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.log.commit {
			n.log.commit = c
			n.hardDirty = true
		}
	}
	n.send(Message{
		Type: MsgAppendResponse, To: m.From, Term: n.term,
		Success: true, MatchIndex: lastNew,
	})
}

func (n *Node) handleAppendResponse(m Message) {
	if n.role != Leader || m.Term < n.term {
		return
	}
	if m.Success {
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
		}
		n.nextIndex[m.From] = n.matchIndex[m.From] + 1
		n.maybeCommit()
		// keep streaming if the follower is behind
		if n.nextIndex[m.From] <= n.log.lastIndex() {
			n.sendAppend(m.From)
		}
		return
	}
	// rejected: back up using the follower's hint and retry immediately
	next := n.nextIndex[m.From] - 1
	if m.RejectHint+1 < next {
		next = m.RejectHint + 1
	}
	if next < 1 {
		next = 1
	}
	n.nextIndex[m.From] = next
	n.sendAppend(m.From)
}

// maybeCommit advances commitIndex to the highest N replicated on a quorum
// with log[N].term == currentTerm (§5.4.2).
func (n *Node) maybeCommit() {
	for N := n.log.lastIndex(); N > n.log.commit; N-- {
		if n.log.term(N) != n.term {
			break // older-term entries commit only transitively
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.quorum() {
			n.log.commit = N
			n.hardDirty = true
			// push the new commit index to followers immediately rather
			// than waiting for the next heartbeat
			n.broadcastAppend()
			break
		}
	}
}

// Ready drains everything the host must now do. HardState and AppendEntries
// must be persisted before Messages are sent (§5.1 durability rule).
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.outMsgs, AppendEntries: n.toAppend}
	n.outMsgs = nil
	n.toAppend = nil
	if n.hardDirty {
		rd.HardState = &HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.log.commit}
		n.hardDirty = false
	}
	if n.softDirty {
		rd.SoftState = &SoftState{Role: n.role, Leader: n.leader, Term: n.term}
		n.softDirty = false
	}
	if n.log.commit > n.log.applied {
		rd.CommittedSlice = n.log.slice(n.log.applied, n.log.commit)
		n.log.applied = n.log.commit
	}
	return rd
}

// ---- observability (the lab UI feeds on this) ----

type Status struct {
	ID         int            `json:"id"`
	Role       string         `json:"role"`
	Term       uint64         `json:"term"`
	VotedFor   int            `json:"votedFor"`
	Leader     int            `json:"leader"`
	Commit     uint64         `json:"commit"`
	Applied    uint64         `json:"applied"`
	LastIndex  uint64         `json:"lastIndex"`
	LogTerms   []uint64       `json:"logTerms"` // term of every entry, for log visualization
	MatchIndex map[int]uint64 `json:"matchIndex,omitempty"`
}

func (n *Node) Status() Status {
	s := Status{
		ID: n.cfg.ID, Role: n.role.String(), Term: n.term, VotedFor: n.votedFor,
		Leader: n.leader, Commit: n.log.commit, Applied: n.log.applied,
		LastIndex: n.log.lastIndex(),
	}
	s.LogTerms = make([]uint64, len(n.log.entries))
	for i, e := range n.log.entries {
		s.LogTerms[i] = e.Term
	}
	if n.role == Leader {
		s.MatchIndex = n.matchIndex
	}
	return s
}

// Entries exposes the full log for persistence and invariant checking.
func (n *Node) Entries() []Entry { return n.log.entries }

// CommitIndex exposes the commit index for invariant checking.
func (n *Node) CommitIndex() uint64 { return n.log.commit }
