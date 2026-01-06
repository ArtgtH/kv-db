package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Node struct {
	mu sync.Mutex

	id    string
	peers []Peer

	store *Store
	fsm   FSM

	state State

	currentTerm uint64
	votedFor    string
	leaderID    string

	log []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	waiters map[uint64]chan struct{}

	http *http.Client

	resetElectionCh chan struct{}
	applyCh         chan struct{}
	stopCh          chan struct{}
	stopped         chan struct{}

	statusSnap atomic.Value // stores Status
}

func NewNode(id string, peers []Peer, store *Store, fsm FSM) (*Node, error) {
	st, entries, err := store.Load()
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:    id,
		peers: peers,
		store: store,
		fsm:   fsm,

		state: Follower,

		currentTerm: st.CurrentTerm,
		votedFor:    st.VotedFor,
		commitIndex: st.CommitIndex,
		lastApplied: st.LastApplied,

		log: entries,

		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		waiters:    map[uint64]chan struct{}{},

		http: &http.Client{Timeout: 2 * time.Second},

		resetElectionCh: make(chan struct{}, 1),
		applyCh:         make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		stopped:         make(chan struct{}),
	}

	n.mu.Lock()
	n.clampIndexesLocked()
	_ = n.persistStateLocked()

	// !!! ВАЖНО для восстановления !!!
	// Если мы перезапустились и commitIndex > lastApplied — нужно применить committed entries.
	n.applyCommittedOnStartLocked()

	n.updateStatusLocked()
	n.mu.Unlock()

	go n.run()
	go n.applyLoop()

	return n, nil
}

func (n *Node) Stop() {
	close(n.stopCh)
	<-n.stopped
}

func (n *Node) Status() Status {
	if v := n.statusSnap.Load(); v != nil {
		return v.(Status)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.statusLocked()
}

func (n *Node) LeaderID() string { return n.Status().LeaderID }
func (n *Node) IsLeader() bool   { return n.Status().State == Leader }

func (n *Node) Propose(entryType string, data []byte) (uint64, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, errors.New("not leader")
	}

	idx := n.lastLogIndex() + 1
	e := LogEntry{Index: idx, Term: n.currentTerm, Type: entryType, Data: data}

	if err := n.store.Append(e); err != nil {
		n.mu.Unlock()
		return 0, err
	}
	n.log = append(n.log, e)

	ch := make(chan struct{})
	n.waiters[idx] = ch

	n.updateStatusLocked()
	n.mu.Unlock()

	// replicate
	n.broadcastAppend()

	select {
	case <-ch:
		return idx, nil
	case <-time.After(10 * time.Second):
		return 0, errors.New("commit timeout")
	}
}

func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		n.updateStatusLocked()
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term, "")
	}

	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		_ = n.persistStateLocked()
		n.resetElectionTimer()
		n.updateStatusLocked()
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}

	n.updateStatusLocked()
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		n.updateStatusLocked()
		return AppendEntriesReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term, args.LeaderID)
	} else {
		if n.state != Follower {
			n.state = Follower
		}
		n.leaderID = args.LeaderID
	}

	n.resetElectionTimer()

	// consistency check
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.lastLogIndex() {
			n.updateStatusLocked()
			return AppendEntriesReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
		}
		if n.termAt(args.PrevLogIndex) != args.PrevLogTerm {
			n.updateStatusLocked()
			return AppendEntriesReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
		}
	}

	// merge entries: delete conflicts and append
	if len(args.Entries) > 0 {
		for _, ne := range args.Entries {
			if ne.Index <= n.lastLogIndex() && n.termAt(ne.Index) != ne.Term {
				if err := n.store.TruncateSuffix(ne.Index - 1); err != nil {
					n.updateStatusLocked()
					return AppendEntriesReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
				}
				n.log = n.store.Log()
				break
			}
		}

		var toAppend []LogEntry
		for _, ne := range args.Entries {
			if ne.Index > n.lastLogIndex() {
				toAppend = append(toAppend, ne)
			}
		}
		if len(toAppend) > 0 {
			if err := n.store.Append(toAppend...); err != nil {
				n.updateStatusLocked()
				return AppendEntriesReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
			}
			n.log = n.store.Log()
		}

		n.clampIndexesLocked()
	}

	// commit update
	if args.LeaderCommit > n.commitIndex {
		last := n.lastLogIndex()
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
		if n.lastApplied > n.commitIndex {
			n.lastApplied = n.commitIndex
		}
		_ = n.persistStateLocked()
		n.signalApply()
	}

	n.updateStatusLocked()
	return AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: n.lastLogIndex()}
}

func (n *Node) run() {
	defer close(n.stopped)

	rand.Seed(time.Now().UnixNano())
	electionTimer := time.NewTimer(n.randElectionTimeout())
	heartbeatTicker := time.NewTicker(120 * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.resetElectionCh:
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(n.randElectionTimeout())

		case <-electionTimer.C:
			n.startElection()
			electionTimer.Reset(n.randElectionTimeout())

		case <-heartbeatTicker.C:
			if n.IsLeader() {
				n.broadcastAppend()
			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	_ = n.persistStateLocked()
	n.updateStatusLocked()

	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	n.mu.Unlock()

	votes := 1
	need := n.majority()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	var muVotes sync.Mutex

	for _, p := range n.peers {
		if p.ID == n.id {
			continue
		}
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if err := n.postJSON(ctx, "http://"+peer.Addr+"/internal/raft/request_vote", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term, "")
				n.updateStatusLocked()
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				muVotes.Lock()
				votes++
				muVotes.Unlock()
			}
		}(p)
	}

	wg.Wait()

	won := false
	n.mu.Lock()
	if n.state == Candidate && n.currentTerm == term && votes >= need {
		n.becomeLeaderLocked()
		won = true
	}
	n.updateStatusLocked()
	n.mu.Unlock()

	if won {
		n.broadcastAppend()
	}
}

func (n *Node) broadcastAppend() {
	n.mu.Lock()
	if n.state != Leader {
		n.updateStatusLocked()
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	lastIdx := n.lastLogIndex()

	// init nextIndex on first leadership
	if len(n.nextIndex) == 0 {
		for _, p := range n.peers {
			if p.ID == n.id {
				continue
			}
			n.nextIndex[p.ID] = lastIdx + 1
			n.matchIndex[p.ID] = 0
		}
	}
	n.updateStatusLocked()
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range n.peers {
		if p.ID == n.id {
			continue
		}
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			n.replicateToPeer(peer, term, leaderID, leaderCommit)
		}(p)
	}
	wg.Wait()

	n.mu.Lock()
	n.advanceCommitLocked()
	n.updateStatusLocked()
	n.mu.Unlock()
}

func (n *Node) replicateToPeer(peer Peer, term uint64, leaderID string, leaderCommit uint64) {
	n.mu.Lock()
	next, ok := n.nextIndex[peer.ID]
	if !ok || next == 0 {
		next = n.lastLogIndex() + 1
		n.nextIndex[peer.ID] = next
		n.matchIndex[peer.ID] = 0
	}
	prevIdx := next - 1
	prevTerm := uint64(0)
	if prevIdx > 0 {
		prevTerm = n.termAt(prevIdx)
	}
	entries := n.entriesFrom(next)
	n.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	var reply AppendEntriesReply
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	if err := n.postJSON(ctx, "http://"+peer.Addr+"/internal/raft/append_entries", args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term, "")
		n.updateStatusLocked()
		return
	}
	if n.state != Leader || n.currentTerm != term {
		n.updateStatusLocked()
		return
	}
	if reply.Success {
		n.matchIndex[peer.ID] = reply.MatchIndex
		n.nextIndex[peer.ID] = reply.MatchIndex + 1
	} else {
		if n.nextIndex[peer.ID] > 1 {
			n.nextIndex[peer.ID]--
		}
	}
	n.updateStatusLocked()
}

func (n *Node) advanceCommitLocked() {
	if n.state != Leader {
		return
	}
	last := n.lastLogIndex()
	for N := last; N > n.commitIndex; N-- {
		if n.termAt(N) != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range n.peers {
			if p.ID == n.id {
				continue
			}
			if n.matchIndex[p.ID] >= N {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = N
			_ = n.persistStateLocked()
			n.signalCommitLocked()
			n.signalApply()
			break
		}
	}
}

func (n *Node) signalCommitLocked() {
	for idx, ch := range n.waiters {
		if idx <= n.commitIndex {
			close(ch)
			delete(n.waiters, idx)
		}
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
			for {
				var next uint64
				var entry LogEntry

				n.mu.Lock()
				if n.lastApplied >= n.commitIndex {
					n.updateStatusLocked()
					n.mu.Unlock()
					break
				}
				next = n.lastApplied + 1
				if next == 0 || next > n.lastLogIndex() {
					n.clampIndexesLocked()
					_ = n.persistStateLocked()
					n.updateStatusLocked()
					n.mu.Unlock()
					break
				}
				entry = n.entryAt(next)
				n.mu.Unlock()

				if err := n.fsm.Apply(entry); err != nil {
					time.Sleep(50 * time.Millisecond)
					n.signalApply()
					break
				}

				n.mu.Lock()
				if next > n.lastApplied {
					n.lastApplied = next
				}
				_ = n.persistStateLocked()
				n.updateStatusLocked()
				n.mu.Unlock()
			}
		}
	}
}

func (n *Node) applyCommittedOnStartLocked() {
	// применяем committed entries, если lastApplied отстаёт
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		if next == 0 || next > n.lastLogIndex() {
			n.clampIndexesLocked()
			_ = n.persistStateLocked()
			return
		}
		entry := n.entryAt(next)
		n.mu.Unlock()
		err := n.fsm.Apply(entry)
		n.mu.Lock()
		if err != nil {
			// на старте лучше не падать: данные могут догнаться после,
			// но обычно Apply должен быть детерминированным и без ошибок.
			return
		}
		n.lastApplied = next
		_ = n.persistStateLocked()
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = leaderID
	_ = n.persistStateLocked()
	n.resetElectionTimer()
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	_ = n.persistStateLocked()
}

func (n *Node) persistStateLocked() error {
	return n.store.SaveState(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	})
}

func (n *Node) clampIndexesLocked() {
	last := n.lastLogIndex()
	if n.commitIndex > last {
		n.commitIndex = last
	}
	if n.lastApplied > n.commitIndex {
		n.lastApplied = n.commitIndex
	}
}

func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 || int(index) > len(n.log) {
		return 0
	}
	return n.log[index-1].Term
}

func (n *Node) entryAt(index uint64) LogEntry {
	return n.log[index-1]
}

func (n *Node) entriesFrom(next uint64) []LogEntry {
	if next == 0 {
		next = 1
	}
	if next > n.lastLogIndex() {
		return nil
	}
	cp := make([]LogEntry, len(n.log[next-1:]))
	copy(cp, n.log[next-1:])
	return cp
}

func (n *Node) majority() int {
	return (len(n.peers) / 2) + 1
}

func (n *Node) randElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (n *Node) postJSON(ctx context.Context, url string, in any, out any) error {
	b, _ := json.Marshal(in)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("rpc failed")
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (n *Node) statusLocked() Status {
	return Status{
		State:       n.state,
		Term:        n.currentTerm,
		LeaderID:    n.leaderID,
		CommitIndex: n.commitIndex,
		LastIndex:   n.lastLogIndex(),
		LastApplied: n.lastApplied,
	}
}

func (n *Node) updateStatusLocked() {
	n.statusSnap.Store(n.statusLocked())
}
