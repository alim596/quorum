package raft

// raftLog is the in-memory replicated log. Index 0 is a virtual empty entry
// (term 0); real entries start at index 1. Snapshots/compaction are out of
// scope — the honest simplification for a lab-scale cluster.
type raftLog struct {
	entries []Entry // entries[i] has Index == uint64(i+1)
	commit  uint64
	applied uint64
}

func newLog(restored []Entry) *raftLog {
	return &raftLog{entries: restored}
}

func (l *raftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 { return l.term(l.lastIndex()) }

// term returns the term of the entry at index i, or 0 for i == 0 / unknown.
func (l *raftLog) term(i uint64) uint64 {
	if i == 0 || i > l.lastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

func (l *raftLog) entry(i uint64) Entry { return l.entries[i-1] }

// slice returns entries in (from, to] — 1-based, inclusive of to.
func (l *raftLog) slice(fromExcl, toIncl uint64) []Entry {
	if fromExcl >= toIncl || fromExcl >= l.lastIndex() {
		return nil
	}
	if toIncl > l.lastIndex() {
		toIncl = l.lastIndex()
	}
	out := make([]Entry, toIncl-fromExcl)
	copy(out, l.entries[fromExcl:toIncl])
	return out
}

func (l *raftLog) append(e Entry) { l.entries = append(l.entries, e) }

// truncateFrom drops entry i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.lastIndex() {
		l.entries = l.entries[:i-1]
	}
}

// upToDate implements the §5.4.1 voting restriction: is a candidate whose
// log ends at (idx, term) at least as up-to-date as ours?
func (l *raftLog) upToDate(idx, term uint64) bool {
	if term != l.lastTerm() {
		return term > l.lastTerm()
	}
	return idx >= l.lastIndex()
}
