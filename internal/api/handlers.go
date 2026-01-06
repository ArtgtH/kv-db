package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"kvraft/internal/raft"
)

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.raft.Status()
	leaderAddr := s.leaderAddr(st.LeaderID)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"node_id":      s.nodeID,
		"state":        st.State,
		"term":         st.Term,
		"leader_id":    st.LeaderID,
		"leader_addr":  leaderAddr,
		"commit":       st.CommitIndex,
		"last_index":   st.LastIndex,
		"last_applied": st.LastApplied,
	})
}

func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "bad key", http.StatusBadRequest)
		return
	}

	if !s.raft.IsLeader() {
		s.forwardToLeader(w, r, 0)
		return
	}

	switch r.Method {
	case http.MethodGet:
		v, ok := s.kv.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"key": key, "value": v})
		return

	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		var in struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			in.Value = string(bytes.TrimSpace(body))
		}

		cmd := map[string]string{"key": key, "value": in.Value}
		b, _ := json.Marshal(cmd)

		if _, err := s.raft.Propose("set", b); err != nil {
			http.Error(w, "propose failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		return

	case http.MethodDelete:
		cmd := map[string]string{"key": key}
		b, _ := json.Marshal(cmd)

		if _, err := s.raft.Propose("del", b); err != nil {
			http.Error(w, "propose failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		return

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) raftStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.raft.Status())
}

func (s *Server) raftRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	reply := s.raft.RequestVote(args)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}

func (s *Server) raftAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	reply := s.raft.AppendEntries(args)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}

func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, depth int) {
	if depth > 2 {
		http.Error(w, "leader forwarding loop", http.StatusServiceUnavailable)
		return
	}

	st := s.raft.Status()
	leaderAddr := s.leaderAddr(st.LeaderID)
	if leaderAddr == "" {
		w.Header().Set("X-Raft-Leader-ID", st.LeaderID)
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}

	if st.LeaderID == s.nodeID && s.raft.IsLeader() {
		http.Error(w, "unexpected leader-forward path", http.StatusServiceUnavailable)
		return
	}

	u := "http://" + leaderAddr + r.URL.Path
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, u, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "proxy build: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))

	resp, err := s.http.Do(req)
	if err != nil {
		http.Error(w, "proxy failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("X-Raft-Leader-ID", st.LeaderID)
	w.Header().Set("X-Raft-Leader-Addr", leaderAddr)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) leaderAddr(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	n, ok := s.cfg.Nodes[leaderID]
	if !ok {
		return ""
	}
	return n.HTTPAddr
}
