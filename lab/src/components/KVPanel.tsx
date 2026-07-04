import { useState } from "react";
import type { Snapshot } from "../lib/types";
import { post } from "../lib/useLab";

export function KVPanel({ snap }: { snap: Snapshot }) {
  const [key, setKey] = useState("color");
  const [value, setValue] = useState("teal");
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);

  const send = async (op: "put" | "delete") => {
    setResult(null);
    const res = await post("/api/kv", { op, key: key.trim(), value });
    const body = (await res.json()) as { leader?: number; index?: number; error?: string };
    if (res.ok) {
      setResult({ ok: true, text: `${op} proposed to n${body.leader} @ log index ${body.index}` });
    } else {
      setResult({ ok: false, text: body.error ?? `HTTP ${res.status}` });
    }
  };

  // replica agreement: value of the demo key on each node
  const leaderReplica = snap.replicas.find(
    (r) => r.node === snap.nodes.find((n) => n.role === "leader")?.id,
  );

  return (
    <section className="panel">
      <div className="panel-head">
        <span className="panel-title">Replicated KV</span>
        <span className="micro">
          {leaderReplica ? `${Object.keys(leaderReplica.data).length} keys` : ""}
        </span>
      </div>
      <div className="kv-form">
        <input type="text" placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} />
        <input type="text" placeholder="value" value={value} onChange={(e) => setValue(e.target.value)} />
        <div className="btn-row full">
          <button className="primary" onClick={() => void send("put")} disabled={!key.trim()}>
            PUT
          </button>
          <button onClick={() => void send("delete")} disabled={!key.trim()}>
            DELETE
          </button>
        </div>
      </div>
      {result && <div className={`kv-result ${result.ok ? "ok" : "err"}`}>{result.text}</div>}
      <div className="kv-note">
        Writes are proposed to the current leader and committed once a quorum
        replicates them. Try writing during a partition — a minority-side
        leader will accept the proposal but can never commit it.
      </div>
    </section>
  );
}
