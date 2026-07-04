package cluster

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/alim596/quorum/internal/raft"
)

// This is the deterministic simulation harness: for each seed we generate a
// random chaos schedule (crashes, restarts, partitions, packet loss, latency
// spikes, client proposals), run the cluster through it tick by tick, heal,
// let it stabilize, and then assert the Raft paper's safety properties held
// throughout. A failing seed reproduces exactly — rerun with the same seed
// and step through it.

const (
	simNodes      = 5
	chaosTicks    = 1200
	settleTicks   = 400
)

func runSeed(t *testing.T, seed int64) {
	t.Helper()
	c := New(seed, simNodes, nil, nil)
	c.Record = true
	rng := rand.New(rand.NewSource(seed ^ 0x5117))

	proposals := 0
	for tick := 0; tick < chaosTicks; tick++ {
		// chaos schedule
		switch r := rng.Float64(); {
		case r < 0.004:
			c.Crash(c.IDs[rng.Intn(len(c.IDs))])
		case r < 0.012:
			for _, id := range c.IDs {
				if c.Net.IsDown(id) {
					c.Restart(id)
					break
				}
			}
		case r < 0.016:
			// random two-group partition
			mid := 1 + rng.Intn(simNodes-1)
			perm := rng.Perm(simNodes)
			var a, b []int
			for i, p := range perm {
				if i < mid {
					a = append(a, c.IDs[p])
				} else {
					b = append(b, c.IDs[p])
				}
			}
			c.Partition([][]int{a, b})
		case r < 0.022:
			c.Heal()
		case r < 0.026:
			c.Net.SetLoss(rng.Float64() * 0.25)
		case r < 0.030:
			min := 1 + uint64(rng.Intn(3))
			c.Net.SetLatency(min, min+uint64(rng.Intn(6)))
		case r < 0.20:
			// a client somewhere writes something
			if _, _, err := c.Propose([]byte(fmt.Sprintf("k%d=v%d", rng.Intn(20), proposals))); err == nil {
				proposals++
			}
		}
		c.Step()
	}

	// heal everything and let the cluster converge
	c.Heal()
	c.Net.SetLoss(0)
	c.Net.SetLatency(1, 3)
	for _, id := range c.IDs {
		if c.Net.IsDown(id) {
			c.Restart(id)
		}
	}
	for i := 0; i < settleTicks; i++ {
		c.Step()
	}

	checkElectionSafety(t, seed, c)
	checkLogMatching(t, seed, c)
	checkAppliedPrefix(t, seed, c)
	checkConvergence(t, seed, c, proposals)
}

// Election safety (§5.2): at most one leader per term, across all history.
func checkElectionSafety(t *testing.T, seed int64, c *Cluster) {
	for term, ids := range c.LeadersByTerm {
		if len(ids) > 1 {
			t.Fatalf("seed %d: ELECTION SAFETY violated — term %d had leaders %v", seed, term, ids)
		}
	}
}

// Log matching (§5.3): same index+term ⇒ identical entries up to that index.
func checkLogMatching(t *testing.T, seed int64, c *Cluster) {
	for i := 0; i < len(c.IDs); i++ {
		for j := i + 1; j < len(c.IDs); j++ {
			a, b := c.Node(c.IDs[i]), c.Node(c.IDs[j])
			if a == nil || b == nil {
				continue
			}
			ea, eb := a.Entries(), b.Entries()
			n := len(ea)
			if len(eb) < n {
				n = len(eb)
			}
			// find the highest common (index,term) point
			match := -1
			for k := n - 1; k >= 0; k-- {
				if ea[k].Term == eb[k].Term {
					match = k
					break
				}
			}
			for k := 0; k <= match; k++ {
				if ea[k].Term != eb[k].Term || !bytes.Equal(ea[k].Data, eb[k].Data) {
					t.Fatalf("seed %d: LOG MATCHING violated — nodes %d,%d diverge at index %d below matching suffix",
						seed, c.IDs[i], c.IDs[j], k+1)
				}
			}
		}
	}
}

// State machine safety (§5.4.3): applied sequences are prefix-compatible.
// (Restarted nodes replay from scratch; comparison is on whatever each node
// applied since its last boot, which must still be a window of one canonical
// sequence — we verify via (index, term, data) agreement at equal indexes.)
func checkAppliedPrefix(t *testing.T, seed int64, c *Cluster) {
	byIndex := map[uint64]raft.Entry{}
	for id, applied := range c.Applied {
		for _, e := range applied {
			if prev, ok := byIndex[e.Index]; ok {
				if prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data) {
					t.Fatalf("seed %d: STATE MACHINE SAFETY violated — node %d applied a different entry at index %d",
						seed, id, e.Index)
				}
			} else {
				byIndex[e.Index] = e
			}
		}
	}
}

// After healing, the cluster must elect a leader and all nodes converge to
// the same commit index (liveness under fair conditions).
func checkConvergence(t *testing.T, seed int64, c *Cluster, proposals int) {
	leaders := 0
	var commit uint64
	first := true
	for _, s := range c.Statuses() {
		if s.Role == "leader" {
			leaders++
		}
		if first {
			commit = s.Commit
			first = false
		} else if s.Commit != commit {
			t.Fatalf("seed %d: no convergence — commit indexes differ after settle (%d vs %d)", seed, s.Commit, commit)
		}
	}
	if leaders != 1 {
		t.Fatalf("seed %d: expected exactly 1 leader after settle, got %d", seed, leaders)
	}
	if proposals > 0 && commit == 0 {
		t.Fatalf("seed %d: %d proposals made but nothing ever committed", seed, proposals)
	}
}

func TestSimulation(t *testing.T) {
	seeds := int64(40)
	if testing.Short() {
		seeds = 8
	}
	for seed := int64(1); seed <= seeds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSeed(t, seed)
		})
	}
}

// TestDeterminism: the same seed must produce the exact same history —
// otherwise failing seeds can't be replayed and the whole approach is moot.
func TestDeterminism(t *testing.T) {
	run := func() ([]Event, []raft.Status) {
		c := New(99, simNodes, nil, nil)
		rng := rand.New(rand.NewSource(99))
		for tick := 0; tick < 600; tick++ {
			if rng.Float64() < 0.1 {
				c.Propose([]byte(fmt.Sprintf("v%d", tick)))
			}
			if tick == 200 {
				c.Partition([][]int{{1, 2}, {3, 4, 5}})
			}
			if tick == 400 {
				c.Heal()
			}
			c.Step()
		}
		return c.Events(), c.Statuses()
	}
	e1, s1 := run()
	e2, s2 := run()
	if fmt.Sprint(e1) != fmt.Sprint(e2) {
		t.Fatal("event history differs between identical seeds")
	}
	if fmt.Sprint(s1) != fmt.Sprint(s2) {
		t.Fatal("final statuses differ between identical seeds")
	}
}
