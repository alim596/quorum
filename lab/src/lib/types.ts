// Wire types mirroring the Go lab API.

export interface NodeStatus {
  id: number;
  role: "leader" | "candidate" | "follower" | "down";
  term: number;
  votedFor: number;
  leader: number;
  commit: number;
  applied: number;
  lastIndex: number;
  logTerms: number[];
  matchIndex?: Record<number, number>;
}

export interface InflightMsg {
  from: number;
  to: number;
  type: string;
  term: number;
  entries: number;
  sentAt: number;
  deliverAt: number;
}

export interface Replica {
  node: number;
  applied: number;
  data: Record<string, string>;
}

export interface LabEvent {
  tick: number;
  node: number;
  kind: string;
  text: string;
  term?: number;
}

export interface Snapshot {
  tick: number;
  tickMs: number;
  nodes: NodeStatus[];
  down: Record<number, boolean>;
  groups: Record<number, number>;
  loss: number;
  latMin: number;
  latMax: number;
  sent: number;
  dropped: number;
  inflight?: InflightMsg[];
  replicas: Replica[];
  events?: LabEvent[];
}
