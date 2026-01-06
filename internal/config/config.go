package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Node struct {
	ID       string `json:"id"`
	HTTPAddr string `json:"http_addr"` // внутри docker-сети: "kv1:8080"
}

type Cluster struct {
	Nodes map[string]Node `json:"nodes"`
}

func Load(path string) (*Cluster, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Cluster
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if len(c.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes in config")
	}
	for k, n := range c.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("node %q has empty id", k)
		}
		if n.HTTPAddr == "" {
			return nil, fmt.Errorf("node %q has empty http_addr", k)
		}
	}
	return &c, nil
}
