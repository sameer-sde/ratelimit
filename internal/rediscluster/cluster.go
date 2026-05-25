// Package rediscluster wraps multiple Redis clients behind a consistent hash
// ring. Callers ask "give me the client for this key" and the cluster picks
// the right shard.
package rediscluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sameer-sde/ratelimit/internal/hashring"
	"github.com/redis/go-redis/v9"
)

// Node represents one Redis instance the cluster knows about.
type Node struct {
	Name   string
	Addr   string
	Client *redis.Client
}

// Cluster routes operations to one of N Redis instances using a consistent
// hash ring.
type Cluster struct {
	ring  *hashring.Ring
	mu    sync.RWMutex
	nodes map[string]*Node
}

// New connects to each address and verifies it's reachable.
// Day 14 tuning: bigger pool (50 vs default 10), warm idle conns, fail-fast
// pool timeout. Under high concurrency the default 10-conn pool becomes the
// bottleneck; bumping it lets more goroutines hit Redis in parallel.
func New(addrs map[string]string) (*Cluster, error) {
	ring := hashring.New(150)
	nodes := make(map[string]*Node, len(addrs))

	for name, addr := range addrs {
		c := redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     50,
			MinIdleConns: 10,
			PoolTimeout:  2 * time.Second,
		})
		if err := c.Ping(context.Background()).Err(); err != nil {
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
func (c *Cluster) ClientFor(key string) *redis.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := c.ring.Get(key)
	if name == "" {
		panic("rediscluster: empty cluster — no nodes registered")
	}
	return c.nodes[name].Client
}

// NodeFor returns the logical node name responsible for a key.
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

// Close shuts down every Redis client.
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
