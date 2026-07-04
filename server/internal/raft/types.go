// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014) as a pure state
// machine.
//
// The design follows the etcd-raft school: a Node has no goroutines, no
// timers, and does no I/O. The host drives it with Tick() (logical time),
// Step() (incoming messages), and Propose(), then drains a Ready() struct
// describing what must happen next: messages to send, state to persist,
// entries to apply. Because the host owns time and delivery order, the exact
// same consensus code runs under the live cluster driver *and* under a
// deterministic, seeded simulation — which is what lets CI hammer it with
// thousands of elections, partitions, and crashes and assert the paper's
// safety properties hold.
package raft

import "fmt"

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// Entry is one slot in the replicated log. Index is 1-based; index 0 is the
// implicit empty entry with term 0.
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

type MsgType uint8

const (
	MsgVoteRequest MsgType = iota
	MsgVoteResponse
	MsgAppendRequest  // also the heartbeat when Entries is empty
	MsgAppendResponse
)

func (t MsgType) String() string {
	switch t {
	case MsgVoteRequest:
		return "RequestVote"
	case MsgVoteResponse:
		return "VoteResponse"
	case MsgAppendRequest:
		return "AppendEntries"
	default:
		return "AppendResponse"
	}
}

// Message is the single wire type exchanged between peers.
type Message struct {
	Type MsgType `json:"type"`
	From int     `json:"from"`
	To   int     `json:"to"`
	Term uint64  `json:"term"`

	// vote request
	LastLogIndex uint64 `json:"lastLogIndex,omitempty"`
	LastLogTerm  uint64 `json:"lastLogTerm,omitempty"`
	// vote response
	Granted bool `json:"granted,omitempty"`

	// append request
	PrevLogIndex uint64  `json:"prevLogIndex,omitempty"`
	PrevLogTerm  uint64  `json:"prevLogTerm,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leaderCommit,omitempty"`
	// append response
	Success    bool   `json:"success,omitempty"`
	MatchIndex uint64 `json:"matchIndex,omitempty"` // on success: last replicated index
	RejectHint uint64 `json:"rejectHint,omitempty"` // on failure: follower's last index (fast backup)
}

func (m Message) String() string {
	return fmt.Sprintf("%s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
}

// HardState is what must be fsynced before acting on a Ready: the paper's
// persistent state (§5.1) plus the log itself (carried separately).
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor int    `json:"votedFor"` // -1 = none
	Commit   uint64 `json:"commit"`   // not strictly required by the paper; speeds recovery
}

// Ready is the host's work order, drained after each Tick/Step/Propose batch.
type Ready struct {
	Messages       []Message // send to peers (after persisting!)
	HardState      *HardState
	AppendEntries  []Entry // stable-store these log entries
	CommittedSlice []Entry // apply these to the state machine, in order
	SoftState      *SoftState
}

// SoftState is volatile, for observability (the lab UI feeds on it).
type SoftState struct {
	Role   Role   `json:"role"`
	Leader int    `json:"leader"` // -1 = unknown
	Term   uint64 `json:"term"`
}
