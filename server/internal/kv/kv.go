// Package kv is the replicated state machine: a key-value store applied
// identically on every node from the committed log. Client commands carry a
// (clientID, seq) pair so retried proposals are applied exactly once — the
// standard Raft session trick (§8).
package kv

import (
	"encoding/json"
	"sync"
)

type Command struct {
	Op       string `json:"op"` // put | delete | cas
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	OldValue string `json:"oldValue,omitempty"` // cas: expected current value
	ClientID string `json:"clientId"`
	Seq      uint64 `json:"seq"`
}

func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Store holds one replica of the state machine per node, so the lab can
// show replicas converging (and diverging replicas would be a caught bug).
type Store struct {
	mu      sync.Mutex
	data    map[int]map[string]string
	lastSeq map[int]map[string]uint64
	applied map[int]uint64 // last applied log index per node
}

func NewStore(nodeIDs []int) *Store {
	s := &Store{
		data:    map[int]map[string]string{},
		lastSeq: map[int]map[string]uint64{},
		applied: map[int]uint64{},
	}
	for _, id := range nodeIDs {
		s.Reset(id)
	}
	return s
}

// Reset clears a node's replica (crash-restart: the log replays from index 1).
func (s *Store) Reset(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[id] = map[string]string{}
	s.lastSeq[id] = map[string]uint64{}
	s.applied[id] = 0
}

// Apply executes one committed entry on one node's replica.
func (s *Store) Apply(id int, index uint64, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied[id] = index
	if len(data) == 0 {
		return // leader no-op entry
	}
	var c Command
	if json.Unmarshal(data, &c) != nil {
		return
	}
	// exactly-once: drop duplicate (client, seq) deliveries
	if c.ClientID != "" && c.Seq <= s.lastSeq[id][c.ClientID] {
		return
	}
	if c.ClientID != "" {
		s.lastSeq[id][c.ClientID] = c.Seq
	}
	switch c.Op {
	case "put":
		s.data[id][c.Key] = c.Value
	case "delete":
		delete(s.data[id], c.Key)
	case "cas":
		if s.data[id][c.Key] == c.OldValue {
			s.data[id][c.Key] = c.Value
		}
	}
}

// Replica returns a copy of one node's store plus its applied index.
func (s *Store) Replica(id int) (map[string]string, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data[id]))
	for k, v := range s.data[id] {
		out[k] = v
	}
	return out, s.applied[id]
}
