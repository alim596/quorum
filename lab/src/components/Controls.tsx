import { useState } from "react";
import type { Snapshot } from "../lib/types";
import { post } from "../lib/useLab";

export function Controls({ snap }: { snap: Snapshot }) {
  const [loss, setLoss] = useState(Math.round(snap.loss * 100));
  const [lat, setLat] = useState(Number(snap.latMax));
  const [speed, setSpeed] = useState(snap.tickMs);

  const leader = snap.nodes.find((n) => n.role === "leader");
  const ids = snap.nodes.map((n) => n.id);

  const isolateLeader = () => {
    if (!leader) return;
    const rest = ids.filter((id) => id !== leader.id);
    void post("/api/partition", { groups: [[leader.id], rest] });
  };

  const splitBrain = () => {
    const minority = ids.slice(0, Math.floor(ids.length / 2));
    const majority = ids.slice(Math.floor(ids.length / 2));
    void post("/api/partition", { groups: [minority, majority] });
  };

  return (
    <section className="panel">
      <div className="panel-head">
        <span className="panel-title">Chaos Controls</span>
        <span className="micro">break things, on purpose</span>
      </div>
      <div className="controls">
        <div className="btn-row">
          <button className="danger" onClick={isolateLeader} disabled={!leader}>
            Isolate leader
          </button>
          <button className="danger" onClick={splitBrain}>
            Split {Math.floor(ids.length / 2)}|{ids.length - Math.floor(ids.length / 2)}
          </button>
          <button className="primary" onClick={() => void post("/api/heal")}>
            Heal network
          </button>
        </div>

        <div className="slider-row">
          <label>packet loss</label>
          <input
            type="range" min={0} max={50} value={loss}
            onChange={(e) => {
              const v = Number(e.target.value);
              setLoss(v);
              void post("/api/loss", { p: v / 100 });
            }}
          />
          <output>{loss}%</output>
        </div>

        <div className="slider-row">
          <label>latency</label>
          <input
            type="range" min={2} max={12} value={lat}
            onChange={(e) => {
              const v = Number(e.target.value);
              setLat(v);
              void post("/api/latency", { min: 2, max: v });
            }}
          />
          <output>2–{lat}t</output>
        </div>

        <div className="slider-row">
          <label>tick speed</label>
          <input
            type="range" min={20} max={200} step={10} value={speed}
            onChange={(e) => {
              const v = Number(e.target.value);
              setSpeed(v);
              void post("/api/speed", { tickMs: v });
            }}
          />
          <output>{speed}ms</output>
        </div>
      </div>
    </section>
  );
}
