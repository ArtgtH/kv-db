package kv

import (
	"encoding/json"
	"fmt"
	"sync"

	"kvraft/internal/raft"
)

type FSM struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewFSM() *FSM {
	return &FSM{data: make(map[string]string)}
}

type setCmd struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type delCmd struct {
	Key string `json:"key"`
}

func (f *FSM) Apply(e raft.LogEntry) error {
	switch e.Type {
	case "set":
		var c setCmd
		if err := json.Unmarshal(e.Data, &c); err != nil {
			return err
		}
		if c.Key == "" {
			return fmt.Errorf("empty key")
		}
		f.mu.Lock()
		f.data[c.Key] = c.Value
		f.mu.Unlock()
		return nil

	case "del":
		var c delCmd
		if err := json.Unmarshal(e.Data, &c); err != nil {
			return err
		}
		if c.Key == "" {
			return fmt.Errorf("empty key")
		}
		f.mu.Lock()
		delete(f.data, c.Key)
		f.mu.Unlock()
		return nil

	default:
		// неизвестные команды игнорируем (удобно для расширений)
		return nil
	}
}

func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

func (f *FSM) All() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.data))
	for k, v := range f.data {
		out[k] = v
	}
	return out
}
