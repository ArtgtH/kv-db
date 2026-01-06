package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kvraft/internal/api"
	"kvraft/internal/config"
	"kvraft/internal/kv"
	"kvraft/internal/raft"
)

func main() {
	var (
		configPath = flag.String("config", "/config/cluster.json", "Path to cluster config JSON")
		nodeID     = flag.String("node-id", "", "Node ID from config")
		listenAddr = flag.String("http", ":8080", "HTTP listen address, e.g. :8080")
		dataDir    = flag.String("data-dir", "/data", "Data directory (persistent volume)")
	)
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("missing -node-id")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	self, ok := cfg.Nodes[*nodeID]
	if !ok {
		log.Fatalf("node %q not found in config", *nodeID)
	}

	peers := make([]raft.Peer, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		peers = append(peers, raft.Peer{ID: n.ID, Addr: n.HTTPAddr})
	}

	kvFSM := kv.NewFSM()

	store := raft.NewStore(*dataDir + "/raft")
	node, err := raft.NewNode(self.ID, peers, store, kvFSM)
	if err != nil {
		log.Fatalf("raft.NewNode: %v", err)
	}
	defer node.Stop()

	srv := api.NewServer(api.Options{
		NodeID:     self.ID,
		ListenAddr: *listenAddr,
		Config:     cfg,
		Raft:       node,
		KV:         kvFSM,
	})

	go func() {
		log.Printf("[main] node=%s listen=%s advertise=%s", self.ID, *listenAddr, self.HTTPAddr)
		if err := srv.Start(); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	// graceful stop
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Printf("[main] shutting down...")
	_ = srv.Shutdown()
}
