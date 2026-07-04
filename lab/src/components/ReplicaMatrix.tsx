import type { Snapshot } from "../lib/types";

// One row per node: raft progress + whether its state machine replica has
// converged with the leader's.
export function ReplicaMatrix({ snap }: { snap: Snapshot }) {
  const leader = snap.nodes.find((n) => n.role === "leader");
  const leaderReplica = snap.replicas.find((r) => r.node === leader?.id);
  const leaderJSON = JSON.stringify(leaderReplica?.data ?? {});

  return (
    <section className="panel">
      <div className="panel-head">
        <span className="panel-title">Replicas</span>
        <span className="micro">state machine convergence</span>
      </div>
      <div className="panel-body">
        <table className="matrix">
          <thead>
            <tr>
              <th>Node</th><th>Role</th><th>Term</th><th>Log</th><th>Commit</th><th>Applied</th><th>Replica</th>
            </tr>
          </thead>
          <tbody>
            {snap.nodes.map((n) => {
              const down = snap.down[n.id];
              const rep = snap.replicas.find((r) => r.node === n.id);
              const inSync = !down && JSON.stringify(rep?.data ?? {}) === leaderJSON;
              return (
                <tr key={n.id}>
                  <td>n{n.id}</td>
                  <td className={down ? "sync-down" : ""}>{down ? "down" : n.role}</td>
                  <td className="num">{down ? "—" : n.term}</td>
                  <td className="num">{down ? "—" : n.lastIndex}</td>
                  <td className="num">{down ? "—" : n.commit}</td>
                  <td className="num">{down ? "—" : rep?.applied ?? 0}</td>
                  <td className={down ? "sync-down" : inSync ? "sync-ok" : "sync-lag"}>
                    {down ? "offline" : inSync ? "✓ in sync" : "catching up…"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
