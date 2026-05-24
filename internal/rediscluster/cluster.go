// Package rediscluster wraps multiple Redis clients behind a consistent hash
// ring. Callers ask "give me the client for this key" and the cluster picks
// the right shard.
package rediscluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/sameer-sde/ratelimit/internal/hashring"
	"github.com/redis/go-redis/v9"
)

// Node represents one Redis instance the cluster knows about.
type Node struct {
	Name   string // logical name used on the ring, e.g. "redis-0"
	Addr   string // host:port
	Client *redis.Client
}

// Cluster routes operations to one of N Redis instances using a consistent
// hash ring. Construction is one-shot — call New with the node list, use it.
type Cluster struct {
	ring  *hashring.Ring
	mu    sync.RWMutex
	nodes map[string]*Node // name -> node
}

// New connects to each address and verifies it's reachable. Returns an
// error if any node fails to PING.
func New(addrs map[string]string) (*Cluster, error) {
	ring := hashring.New(150)
	nodes := make(map[string]*Node, len(addrs))

	for name, addr := range addrs {
		c := redis.NewClient(&redis.Options{Addr: addr})
		if err := c.Ping(context.Background()).Err(); err != nil {
			// Best-effort cleanup of clients already created.
			for _, n := range nodes {
				_ = n.Client.Close()
			}
			_ = c.Close()
			return nil, fmt.Errorf("ping %s (%s): %w", name, addr, err)
		}
		nodes[name] = &Node{Name: name, Addr: addr, Client: c}
		ring.Add(name)
	}

	return &Cluster{ring: ring, nodes: nodes}, nil
}

// ClientFor returns the Redis client that should hold state for the given key.
// Same key → same client, always. Panics only if the cluster is empty
// (programmer error during init).
func (c *Cluster) ClientFor(key string) *redis.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := c.ring.Get(key)
	if name == "" {
		panic("rediscluster: empty cluster — no nodes registered")
	}
	return c.nodes[name].Client
}

// NodeFor returns the logical node name responsible for a key. Used by the
// /cluster/info endpoint and the dashboard.
func (c *Cluster) NodeFor(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Get(key)
}

// Nodes returns the names of all registered shards, sorted.
func (c *Cluster) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Nodes()
}

// Close shuts down every Redis client. Call on server shutdown.
func (c *Cluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, n := range c.nodes {
		if err := n.Client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
