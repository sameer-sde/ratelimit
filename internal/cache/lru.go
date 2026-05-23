// Package cache provides an in-memory LRU cache used as a decision cache
// in front of Redis for hot rate-limit keys.
package cache

import (
	"sync"
	"time"
)

// entry is one node in the doubly linked list.
type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
	prev      *entry
	next      *entry
}

// LRU is a thread-safe LRU cache with per-entry TTL.
type LRU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*entry
	head     *entry // most-recently used
	tail     *entry // least-recently used

	// Stats — exposed for the dashboard later.
	hits   uint64
	misses uint64
}

// New returns an LRU of the given capacity.
func New(capacity int) *LRU {
	if capacity <= 0 {
		capacity = 1
	}
	return &LRU{
		capacity: capacity,
		items:    make(map[string]*entry, capacity),
	}
}

// Get returns the cached value for key, or (nil, false) if missing/expired.
func (l *LRU) Get(key string) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e, ok := l.items[key]
	if !ok {
		l.misses++
		return nil, false
	}

	// Expired? Evict and treat as miss.
	if time.Now().After(e.expiresAt) {
		l.removeEntry(e)
		l.misses++
		return nil, false
	}

	l.moveToHead(e)
	l.hits++
	return e.value, true
}

// Put inserts or updates key with the given value and TTL.
func (l *LRU) Put(key string, value []byte, ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expires := time.Now().Add(ttl)

	if e, ok := l.items[key]; ok {
		// Update existing entry in place.
		e.value = value
		e.expiresAt = expires
		l.moveToHead(e)
		return
	}

	// Brand new entry — insert at head.
	e := &entry{key: key, value: value, expiresAt: expires}
	l.items[key] = e
	l.addToHead(e)

	// Evict tail if we're over capacity.
	if len(l.items) > l.capacity {
		l.removeEntry(l.tail)
	}
}

// Stats returns hit/miss counters. Safe for concurrent use.
func (l *LRU) Stats() (hits, misses uint64, size int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hits, l.misses, len(l.items)
}

// --- linked-list helpers (caller must hold l.mu) ---

func (l *LRU) addToHead(e *entry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
}

func (l *LRU) moveToHead(e *entry) {
	if e == l.head {
		return
	}
	// Detach e from its current position.
	if e.prev != nil {
		e.prev.next = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	if e == l.tail {
		l.tail = e.prev
	}
	// Insert at head.
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
}

func (l *LRU) removeEntry(e *entry) {
	if e == nil {
		return
	}
	delete(l.items, e.key)
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}
