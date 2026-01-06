package api

import (
	"context"
	"log"
	"net/http"
	"time"

	"kvraft/internal/config"
	"kvraft/internal/kv"
	"kvraft/internal/raft"
)

type Options struct {
	NodeID     string
	ListenAddr string
	Config     *config.Cluster
	Raft       *raft.Node
	KV         *kv.FSM
}

type Server struct {
	nodeID string
	cfg    *config.Cluster
	raft   *raft.Node
	kv     *kv.FSM

	srv  *http.Server
	http *http.Client
}

func NewServer(opt Options) *Server {
	mux := http.NewServeMux()
	s := &Server{
		nodeID: opt.NodeID,
		cfg:    opt.Config,
		raft:   opt.Raft,
		kv:     opt.KV,
		http:   &http.Client{Timeout: 2 * time.Second},
	}

	// Public
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/status", s.status)
	mux.HandleFunc("/kv/", s.kvHandler) // GET/PUT/DELETE /kv/<key>

	// Internal Raft RPC
	mux.HandleFunc("/internal/raft/status", s.raftStatus)
	mux.HandleFunc("/internal/raft/request_vote", s.raftRequestVote)
	mux.HandleFunc("/internal/raft/append_entries", s.raftAppendEntries)

	s.srv = &http.Server{
		Addr:              opt.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("[api] server created node=%s listen=%s", s.nodeID, opt.ListenAddr)
	return s
}

func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
