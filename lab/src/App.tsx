import { useLab } from "./lib/useLab";
import { ClusterView } from "./components/ClusterView";
import { Controls } from "./components/Controls";
import { KVPanel } from "./components/KVPanel";
import { ReplicaMatrix } from "./components/ReplicaMatrix";
import { Console } from "./components/Console";

export default function App() {
  const { snap, connected } = useLab();

  if (!snap) {
    return (
      <div className="app">
        <header className="header area-head">
          <span className="brand">
            QUO<b>RUM</b>
          </span>
          <span className="brand-sub">raft consensus · chaos lab</span>
          <span className="spacer" />
          <div className="head-cell">
            <span className="k">link</span>
            <span className={`v ${connected ? "ok" : "bad"}`}>{connected ? "CONNECTED" : "CONNECTING…"}</span>
          </div>
        </header>
        <div className="panel area-stage">
          <div className="empty" style={{ marginTop: 60 }}>
            waiting for cluster state — is the server running on :8090?
          </div>
        </div>
      </div>
    );
  }

  const leader = snap.nodes.find((n) => n.role === "leader");
  const dropPct = snap.sent > 0 ? ((100 * snap.dropped) / snap.sent).toFixed(1) : "0.0";

  return (
    <div className="app">
      <header className="header area-head">
        <span className="brand">
          QUO<b>RUM</b>
        </span>
        <span className="brand-sub">raft consensus · chaos lab</span>
        <div className="head-cell">
          <span className="k">leader</span>
          <span className="v">{leader ? `n${leader.id} (term ${leader.term})` : "— electing —"}</span>
        </div>
        <div className="head-cell">
          <span className="k">tick</span>
          <span className="v">{snap.tick}</span>
        </div>
        <div className="head-cell">
          <span className="k">msgs sent / dropped</span>
          <span className="v">
            {snap.sent} / {snap.dropped} ({dropPct}%)
          </span>
        </div>
        <span className="spacer" />
        <div className="head-cell">
          <span className="k">link</span>
          <span className={`v ${connected ? "ok" : "bad"}`}>{connected ? "LIVE" : "RECONNECTING"}</span>
        </div>
      </header>

      <div className="panel area-stage stage">
        <ClusterView snap={snap} />
      </div>

      <aside className="area-side">
        <Controls snap={snap} />
        <KVPanel snap={snap} />
      </aside>

      <div className="area-foot">
        <ReplicaMatrix snap={snap} />
        <Console snap={snap} />
      </div>
    </div>
  );
}
