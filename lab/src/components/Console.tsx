import type { Snapshot } from "../lib/types";

export function Console({ snap }: { snap: Snapshot }) {
  const events = (snap.events ?? []).slice().reverse().slice(0, 60);
  return (
    <section className="panel">
      <div className="panel-head">
        <span className="panel-title">Event Log</span>
        <span className="micro">tick {snap.tick}</span>
      </div>
      <div className="panel-body">
        {events.length === 0 && <div className="empty">quiet so far</div>}
        {events.map((e, i) => (
          <div className="console-line" key={`${e.tick}-${i}`}>
            <span className="tick">t{e.tick}</span>
            <span className={`kind kind-${e.kind}`}>{e.kind}</span>
            <span className="text">{e.text}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
