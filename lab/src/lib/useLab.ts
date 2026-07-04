import { useEffect, useRef, useState } from "react";
import type { Snapshot } from "./types";

// Live cluster state over WebSocket, with auto-reconnect.
export function useLab() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const retry = useRef(500);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/ws/lab`);
      ws.onopen = () => {
        retry.current = 500;
        setConnected(true);
      };
      ws.onmessage = (e) => setSnap(JSON.parse(e.data as string) as Snapshot);
      ws.onclose = () => {
        setConnected(false);
        if (!closed) {
          setTimeout(connect, retry.current);
          retry.current = Math.min(retry.current * 2, 5000);
        }
      };
      ws.onerror = () => ws?.close();
    };
    connect();
    return () => {
      closed = true;
      ws?.close();
    };
  }, []);

  return { snap, connected };
}

// fire-and-forget control calls
export async function post(path: string, body?: unknown): Promise<Response> {
  return fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
