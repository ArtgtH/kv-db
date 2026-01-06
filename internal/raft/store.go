package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type persistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
}

type Store struct {
	dir       string
	state     persistentState
	log       []LogEntry
	stateFile string
	logFile   string
}

func NewStore(dir string) *Store {
	return &Store{
		dir:       dir,
		stateFile: filepath.Join(dir, "state.json"),
		logFile:   filepath.Join(dir, "log.wal"),
	}
}

func (s *Store) Load() (persistentState, []LogEntry, error) {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return persistentState{}, nil, fmt.Errorf("mkdir raft dir %q: %w", s.dir, err)
	}

	// state
	b, err := os.ReadFile(s.stateFile)
	if err == nil {
		_ = json.Unmarshal(b, &s.state)
	}

	// log
	entries, err := s.readLog()
	if err != nil {
		return s.state, nil, err
	}
	s.log = entries
	return s.state, s.log, nil
}

func (s *Store) SaveState(st persistentState) error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("mkdir raft dir %q: %w", s.dir, err)
	}

	s.state = st
	tmp := s.stateFile + ".tmp"
	b, _ := json.Marshal(st)
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.stateFile)
}

func (s *Store) Append(entries ...LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("mkdir raft dir %q: %w", s.dir, err)
	}

	f, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, e := range entries {
		b, _ := json.Marshal(e)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := f.Write(b); err != nil {
			return err
		}
		s.log = append(s.log, e)
	}
	return f.Sync()
}

func (s *Store) TruncateSuffix(index uint64) error {
	var kept []LogEntry
	for _, e := range s.log {
		if e.Index <= index {
			kept = append(kept, e)
		}
	}
	s.log = kept
	return s.rewriteLog()
}

func (s *Store) Log() []LogEntry { return s.log }

func (s *Store) readLog() ([]LogEntry, error) {
	f, err := os.Open(s.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var out []LogEntry
	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(f, lenBuf[:])
		if err != nil {
			break
		}
		l := binary.LittleEndian.Uint32(lenBuf[:])
		buf := make([]byte, l)
		_, err = io.ReadFull(f, buf)
		if err != nil {
			// partial tail -> stop
			break
		}
		var e LogEntry
		if err := json.Unmarshal(buf, &e); err != nil {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *Store) rewriteLog() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("mkdir raft dir %q: %w", s.dir, err)
	}

	tmp := s.logFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	for _, e := range s.log {
		b, _ := json.Marshal(e)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(b); err != nil {
			_ = f.Close()
			return err
		}
	}
	_ = f.Sync()
	_ = f.Close()
	return os.Rename(tmp, s.logFile)
}
