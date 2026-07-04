import { useMemo } from "react";
import type { Snapshot } from "../lib/types";
import { post } from "../lib/useLab";

// The schematic: nodes on a circle, log bars beneath each node, message
// dots in flight between them. Click a node to crash/restart it.

const W = 900;
const H = 640;
const CX = W / 2;
const CY = H / 2 - 20;
const R = 215;

const TERM_COLORS = ["#4fc9e8", "#f0b23e", "#b48af5", "#3fd68f", "#e35b5b", "#e8873f"];

function nodePos(i: number, n: number, group: number): { x: number; y: number } {
  const angle = -Math.PI / 2 + (i * 2 * Math.PI) / n;
  // partitioned groups drift radially outward so the split is visible
  const r = R + (group > 0 ? 46 : 0);
  return { x: CX + r * Math.cos(angle), y: CY + r * Math.sin(angle) };
}

export function ClusterView({ snap }: { snap: Snapshot }) {
  const n = snap.nodes.length;
  const positions = useMemo(() => {
    const map = new Map<number, { x: number; y: number }>();
    snap.nodes.forEach((node, i) => {
      map.set(node.id, nodePos(i, n, snap.groups[node.id] ?? 0));
    });
    return map;
  }, [snap.nodes, snap.groups, n]);

  const partitioned = new Set(Object.values(snap.groups)).size > 1;

  const toggleNode = (id: number) => {
    void post(snap.down[id] ? `/api/restart/${id}` : `/api/crash/${id}`);
  };

  return (
    <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet">
      {/* faint ring guide */}
      <circle cx={CX} cy={CY} r={R} fill="none" stroke="var(--ink-dim)" strokeWidth="0.75" strokeDasharray="2 6" />
      {partitioned && (
        <text x={CX} y={38} textAnchor="middle" fontFamily="var(--sans)" fontSize="13" letterSpacing="0.3em" fill="var(--bad)">
          NETWORK PARTITIONED
        </text>
      )}

      {/* peer links (dashed when severed) */}
      {snap.nodes.map((a, i) =>
        snap.nodes.slice(i + 1).map((b) => {
          const pa = positions.get(a.id)!;
          const pb = positions.get(b.id)!;
          const severed = (snap.groups[a.id] ?? 0) !== (snap.groups[b.id] ?? 0);
          return (
            <line
              key={`${a.id}-${b.id}`}
              x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
              className={severed ? "partition-line" : ""}
              stroke={severed ? undefined : "var(--ink-dim)"}
              strokeWidth={severed ? undefined : 0.6}
              opacity={severed ? undefined : 0.55}
            />
          );
        }),
      )}

      {/* in-flight messages */}
      {(snap.inflight ?? []).map((m, i) => {
        const from = positions.get(m.from);
        const to = positions.get(m.to);
        if (!from || !to) return null;
        const span = Math.max(1, m.deliverAt - m.sentAt);
        let p = (snap.tick - m.sentAt) / span;
        p = Math.max(0.05, Math.min(0.95, p));
        const cls =
          m.type === "RequestVote" ? "msg-vote" :
          m.type === "VoteResponse" ? "msg-voteresp" :
          m.type === "AppendEntries" ? "msg-append" : "msg-appendresp";
        const r = m.type === "AppendEntries" && m.entries > 0 ? 6 : 3.5;
        return (
          <circle
            key={`${m.from}-${m.to}-${m.sentAt}-${i}`}
            className={`msg-dot ${cls}`}
            cx={from.x + (to.x - from.x) * p}
            cy={from.y + (to.y - from.y) * p}
            r={r}
            opacity={0.9}
          />
        );
      })}

      {/* nodes */}
      {snap.nodes.map((node) => {
        const pos = positions.get(node.id)!;
        const down = snap.down[node.id];
        const role = down ? "down" : node.role;
        return (
          <g key={node.id} className="node-hit" onClick={() => toggleNode(node.id)}>
            <circle cx={pos.x} cy={pos.y} r={40} className={`node-ring ${role}`} />
            <text x={pos.x} y={pos.y - 2} className={`node-label ${down ? "down" : ""}`}>
              n{node.id}
            </text>
            <text x={pos.x} y={pos.y + 13} className={`node-role ${role}`}>
              {role.toUpperCase()}
            </text>
            <text x={pos.x} y={pos.y - 52} className="node-sub">
              term {node.term} · commit {node.commit}
            </text>

            {/* log bar: last 14 entries, colored by term, dimmed if uncommitted */}
            <g>
              {(node.logTerms ?? []).slice(-14).map((term, j, arr) => {
                const idx = node.lastIndex - arr.length + j + 1;
                const committed = idx <= node.commit;
                const size = 9;
                const x0 = pos.x - (arr.length * (size + 1)) / 2 + j * (size + 1);
                return (
                  <rect
                    key={idx}
                    className="log-cell"
                    x={x0} y={pos.y + 50}
                    width={size} height={size}
                    fill={committed ? TERM_COLORS[Number(term) % TERM_COLORS.length] : "transparent"}
                    stroke={TERM_COLORS[Number(term) % TERM_COLORS.length]}
                    opacity={committed ? 0.95 : 0.55}
                  />
                );
              })}
            </g>
            <text x={pos.x} y={pos.y + 76} className="node-hint">
              {down ? "CLICK TO RESTART" : "CLICK TO CRASH"}
            </text>
          </g>
        );
      })}

      {/* legend */}
      <g fontFamily="var(--mono)" fontSize="9.5" fill="var(--dim)">
        <circle cx={26} cy={H - 66} r={4} className="msg-append" />
        <text x={38} y={H - 62.5}>AppendEntries / heartbeat</text>
        <circle cx={26} cy={H - 48} r={4} className="msg-vote" />
        <text x={38} y={H - 44.5}>RequestVote</text>
        <rect x={22} y={H - 36} width={9} height={9} fill={TERM_COLORS[1]} />
        <text x={38} y={H - 27.5}>log entry (fill = committed, hue = term)</text>
      </g>
    </svg>
  );
}
