// Command quorum runs a 5-node Raft cluster over a virtual network and
// serves the browser chaos lab.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/alim596/quorum/internal/cluster"
	"github.com/alim596/quorum/internal/kv"
	"github.com/alim596/quorum/internal/lab"
	"github.com/alim596/quorum/internal/raft"
)

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	nodes := flag.Int("nodes", 5, "cluster size (odd numbers make quorums interesting)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "determinism seed")
	flag.Parse()

	ids := make([]int, *nodes)
	for i := range ids {
		ids[i] = i + 1
	}
	store := kv.NewStore(ids)

	c := cluster.New(*seed, *nodes,
		func(id int, e raft.Entry) { store.Apply(id, e.Index, e.Data) },
		func(id int) { store.Reset(id) },
	)

	l := lab.New(c, store)
	go l.Run()

	log.Printf("quorum: %d-node raft cluster running (seed %d)", *nodes, *seed)
	log.Printf("quorum: chaos lab API on %s", *addr)
	if err := http.ListenAndServe(*addr, l.Handler()); err != nil {
		log.Fatal(err)
	}
}
