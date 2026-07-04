// Package lab drives the cluster on a wall clock and exposes it to the
// browser: a WebSocket state stream for the visualization plus REST chaos
// controls. All cluster access is serialized through one mutex around the
// single-threaded runtime — the lab is a thin window onto it, not a second
// brain.
package lab

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alim596/quorum/internal/cluster"
	"github.com/alim596/quorum/internal/kv"
	"github.com/alim596/quorum/internal/raft"
)

type Lab struct {
	mu      sync.Mutex
	c       *cluster.Cluster
	store   *kv.Store
	tickMs  int
	kvSeq   uint64
	started time.Time

	wsMu    sync.Mutex
	clients map[*websocket.Conn]bool
}

func New(c *cluster.Cluster, store *kv.Store) *Lab {
	// Human pacing: 60ms ticks and 2-5 tick latency make elections and
	// message flight visible; the simulation tests run the same cluster
	// code at full speed.
	c.Net.SetLatency(2, 5)
	return &Lab{
		c: c, store: store, tickMs: 60, started: time.Now(),
		clients: map[*websocket.Conn]bool{},
	}
}

// Run drives the cluster and broadcasts snapshots until the process exits.
func (l *Lab) Run() {
	go l.broadcastLoop()
	for {
		l.mu.Lock()
		l.c.Step()
		ms := l.tickMs
		l.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// ---- snapshot ----

type inflightView struct {
	From      int    `json:"from"`
	To        int    `json:"to"`
	Type      string `json:"type"`
	Term      uint64 `json:"term"`
	Entries   int    `json:"entries"`
	SentAt    uint64 `json:"sentAt"`
	DeliverAt uint64 `json:"deliverAt"`
}

type replicaView struct {
	Node    int               `json:"node"`
	Applied uint64            `json:"applied"`
	Data    map[string]string `json:"data"`
}

type snapshot struct {
	Tick     uint64            `json:"tick"`
	TickMs   int               `json:"tickMs"`
	Nodes    []raft.Status     `json:"nodes"`
	Down     map[int]bool      `json:"down"`
	Groups   map[int]int       `json:"groups"`
	Loss     float64           `json:"loss"`
	LatMin   uint64            `json:"latMin"`
	LatMax   uint64            `json:"latMax"`
	Sent     uint64            `json:"sent"`
	Dropped  uint64            `json:"dropped"`
	Inflight []inflightView    `json:"inflight"`
	Replicas []replicaView     `json:"replicas"`
	Events   []cluster.Event   `json:"events"`
}

func (l *Lab) snapshot() snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	net := l.c.Net
	latMin, latMax := net.Latency()
	s := snapshot{
		Tick: net.Now(), TickMs: l.tickMs,
		Nodes: l.c.Statuses(), Groups: net.Groups(),
		Loss: net.Loss(), LatMin: latMin, LatMax: latMax,
		Sent: net.Sent, Dropped: net.Dropped,
		Down:   map[int]bool{},
		Events: l.c.Events(),
	}
	for _, id := range l.c.IDs {
		s.Down[id] = net.IsDown(id)
		data, applied := l.store.Replica(id)
		s.Replicas = append(s.Replicas, replicaView{Node: id, Applied: applied, Data: data})
	}
	for _, inf := range net.Inflight() {
		s.Inflight = append(s.Inflight, inflightView{
			From: inf.Msg.From, To: inf.Msg.To, Type: inf.Msg.Type.String(),
			Term: inf.Msg.Term, Entries: len(inf.Msg.Entries),
			SentAt: inf.SentAt, DeliverAt: inf.DeliverAt,
		})
	}
	return s
}

func (l *Lab) broadcastLoop() {
	t := time.NewTicker(90 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		l.wsMu.Lock()
		if len(l.clients) == 0 {
			l.wsMu.Unlock()
			continue
		}
		payload, err := json.Marshal(l.snapshot())
		if err != nil {
			l.wsMu.Unlock()
			continue
		}
		for conn := range l.clients {
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if conn.WriteMessage(websocket.TextMessage, payload) != nil {
				conn.Close()
				delete(l.clients, conn)
			}
		}
		l.wsMu.Unlock()
	}
}

// ---- HTTP ----

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (l *Lab) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ws/lab", l.handleWS)
	mux.HandleFunc("POST /api/crash/{id}", l.withNodeID(func(id int) { l.c.Crash(id) }))
	mux.HandleFunc("POST /api/restart/{id}", l.withNodeID(func(id int) { l.c.Restart(id) }))
	mux.HandleFunc("POST /api/heal", l.locked(func(w http.ResponseWriter, r *http.Request) {
		l.c.Heal()
		writeJSON(w, 200, ok())
	}))
	mux.HandleFunc("POST /api/partition", l.locked(l.handlePartition))
	mux.HandleFunc("POST /api/loss", l.locked(l.handleLoss))
	mux.HandleFunc("POST /api/latency", l.locked(l.handleLatency))
	mux.HandleFunc("POST /api/speed", l.locked(l.handleSpeed))
	mux.HandleFunc("POST /api/kv", l.locked(l.handleKV))
	return cors(mux)
}

func ok() map[string]bool { return map[string]bool{"ok": true} }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *Lab) locked(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		defer l.mu.Unlock()
		fn(w, r)
	}
}

func (l *Lab) withNodeID(fn func(int)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad node id"})
			return
		}
		l.mu.Lock()
		fn(id)
		l.mu.Unlock()
		writeJSON(w, 200, ok())
	}
}

func (l *Lab) handlePartition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Groups [][]int `json:"groups"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Groups) == 0 {
		writeJSON(w, 400, map[string]string{"error": "groups required, e.g. {\"groups\":[[1,2],[3,4,5]]}"})
		return
	}
	l.c.Partition(body.Groups)
	writeJSON(w, 200, ok())
}

func (l *Lab) handleLoss(w http.ResponseWriter, r *http.Request) {
	var body struct {
		P float64 `json:"p"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	l.c.Net.SetLoss(body.P)
	writeJSON(w, 200, ok())
}

func (l *Lab) handleLatency(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Min uint64 `json:"min"`
		Max uint64 `json:"max"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	l.c.Net.SetLatency(body.Min, body.Max)
	writeJSON(w, 200, ok())
}

func (l *Lab) handleSpeed(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TickMs int `json:"tickMs"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.TickMs < 10 {
		body.TickMs = 10
	}
	if body.TickMs > 300 {
		body.TickMs = 300
	}
	l.tickMs = body.TickMs
	writeJSON(w, 200, ok())
}

func (l *Lab) handleKV(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Op       string `json:"op"`
		Key      string `json:"key"`
		Value    string `json:"value"`
		OldValue string `json:"oldValue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Key == "" {
		writeJSON(w, 400, map[string]string{"error": "op and key required"})
		return
	}
	switch body.Op {
	case "put", "delete", "cas":
	default:
		writeJSON(w, 400, map[string]string{"error": "op must be put, delete, or cas"})
		return
	}
	l.kvSeq++
	cmd := kv.Command{
		Op: body.Op, Key: body.Key, Value: body.Value, OldValue: body.OldValue,
		ClientID: "lab-client", Seq: l.kvSeq,
	}
	leader, index, err := l.c.Propose(cmd.Encode())
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "no leader available — the cluster is mid-election or has no quorum"})
		return
	}
	writeJSON(w, 200, map[string]any{"leader": leader, "index": index})
}

func (l *Lab) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	l.wsMu.Lock()
	l.clients[conn] = true
	l.wsMu.Unlock()
	// reader just detects close
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				l.wsMu.Lock()
				delete(l.clients, conn)
				l.wsMu.Unlock()
				conn.Close()
				return
			}
		}
	}()
}
