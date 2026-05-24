// Package hashring implements a consistent hash ring with virtual nodes.
//
// Used to map rate-limit keys to one of N Redis instances such that
// adding or removing a Redis only displaces ~1/N of keys.
package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent hash ring. Safe for concurrent reads after construction;
// writes (Add/Remove) take a write lock.
type Ring struct {
	mu sync.RWMutex

	virtualPerNode int

	// positions is the sorted ring positions (hash values).
	positions []uint32

	// posToNode maps a position to the node name that owns it.
	posToNode map[uint32]string

	// nodes is the set of physical node names currently in the ring.
	nodes map[string]struct{}
}

// New returns an empty ring. virtualPerNode is how many positions on the
// ring each physical node will occupy. 150 is the common default and gives
// good distribution; 64 is fine for small clusters. Must be > 0.
func New(virtualPerNode int) *Ring {
	if virtualPerNode <= 0 {
		virtualPerNode = 150
	}
	return &Ring{
		virtualPerNode: virtualPerNode,
		posToNode:      make(map[uint32]string),
		nodes:          make(map[string]struct{}),
	}
}

// Add inserts a node into the ring. No-op if the node is already present.
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node]; exists {
		return
	}
	r.nodes[node] = struct{}{}

	for i := 0; i < r.virtualPerNode; i++ {
		// virtual node label, e.g. "redis-0#42"
		label := node + "#" + strconv.Itoa(i)
		pos := crc32.ChecksumIEEE([]byte(label))
		r.positions = append(r.positions, pos)
		r.posToNode[pos] = node
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// Remove deletes a node and all its virtual positions. No-op if absent.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node]; !exists {
		return
	}
	delete(r.nodes, node)

	// Filter positions in place, dropping any belonging to this node.
	kept := r.positions[:0]
	for _, p := range r.positions {
		if r.posToNode[p] == node {
			delete(r.posToNode, p)
			continue
		}
		kept = append(kept, p)
	}
	r.positions = kept
}

// Get returns the node responsible for the given key. Returns "" if the
// ring is empty.
func (r *Ring) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.positions) == 0 {
		return ""
	}

	h := crc32.ChecksumIEEE([]byte(key))

	// Binary search: smallest ring position >= h.
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})
	if idx == len(r.positions) {
		// Wrapped around the ring; first position owns it.
		idx = 0
	}

	return r.posToNode[r.positions[idx]]
}

// Nodes returns the current set of physical node names. Useful for the
// dashboard / debugging.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Size returns the number of physical nodes in the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
