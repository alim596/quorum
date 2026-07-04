# LinkedIn post draft

> Paste-ready. Best asset: a 20–40s screen recording — click the leader dead,
> watch the re-election animation, restart it, show the replica matrix going
> green. Post this a week or two after the Meridian post, not the same day.

---

I implemented Raft consensus from scratch. Then I built a lab for torturing it.

**Quorum** is a distributed key-value store running on my own implementation of the
Raft consensus algorithm — no etcd, no libraries, ~450 lines of Go for the consensus
core — with a browser "chaos lab" where you can:

⚡ Click any node to crash it and watch the cluster elect a new leader in real time

🔪 Partition the network mid-write and watch a minority-side leader accept a proposal
it can never commit — then watch the truth win when the network heals

📉 Crank packet loss to 40% and watch progress get slower but never *wrong*

💾 Restart a dead node and watch it replay its persisted log back into sync

The part I'm proudest of isn't visible in the demo: the consensus core is a pure state
machine — no goroutines, no timers, no I/O inside. The host owns time and message
delivery. Which means the exact same code that drives the live visualization also runs
under a deterministic simulator: every CI push executes 40 seeded chaos schedules
(crashes, partitions, packet loss, concurrent writes) and machine-checks the Raft
paper's safety properties — election safety, log matching, leader completeness, state
machine safety. A failing seed replays identically, every single time. That's the
FoundationDB testing philosophy, applied end to end.

Fun fact: the visualization caught a real bug the unit tests missed (a crashed node
serialized a null log and took the whole UI down with it). Observability is testing.

Repo: github.com/alim596/quorum
Stack: Go · React · TypeScript · SVG · zero consensus dependencies

Companion project — a full exchange with a matching engine doing 2.2M ops/sec:
github.com/alim596/meridian

#golang #distributedsystems #raft #consensus #typescript #softwareengineering #buildinpublic

---

## Shot list for the demo video

1. Full lab, cluster healthy, heartbeat dots pulsing (3s hold).
2. PUT a key — AppendEntries fan-out, log bars fill on all five nodes.
3. Click the leader. Hold through the election: candidate ring pulsing violet,
   RequestVote dots, new amber leader.
4. Replica matrix: four rows "✓ in sync", crashed node "offline".
5. Click the dead node to restart — event log shows "restarted from disk",
   matrix returns to five green rows.
6. "Split 2|3" partition: red dashed severed links, groups drifting apart,
   then Heal and convergence.
7. Terminal: `go test -race ./...` scrolling the 40 chaos seeds, all ok.
