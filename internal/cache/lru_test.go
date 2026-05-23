package cache

import (
	"testing"
	"time"
)

func TestLRU_BasicGetPut(t *testing.T) {
	c := New(3)
	c.Put("a", []byte("1"), time.Minute)
	c.Put("b", []byte("2"), time.Minute)
	c.Put("c", []byte("3"), time.Minute)

	if v, ok := c.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("expected a=1, got ok=%v v=%s", ok, v)
	}
	if v, ok := c.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("expected b=2, got ok=%v v=%s", ok, v)
	}
}

func TestLRU_EvictionOrder(t *testing.T) {
	c := New(2)
	c.Put("a", []byte("1"), time.Minute)
	c.Put("b", []byte("2"), time.Minute)
	// Access "a" so "b" becomes least-recently-used.
	c.Get("a")
	c.Put("c", []byte("3"), time.Minute)
	// "b" should have been evicted, not "a".
	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a to still be cached")
	}
}

func TestLRU_TTLExpiry(t *testing.T) {
	c := New(2)
	c.Put("a", []byte("1"), 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected a to be expired")
	}
}

func TestLRU_UpdateExistingKey(t *testing.T) {
	c := New(2)
	c.Put("a", []byte("1"), time.Minute)
	c.Put("a", []byte("99"), time.Minute) // overwrite
	v, ok := c.Get("a")
	if !ok || string(v) != "99" {
		t.Fatalf("expected a=99, got ok=%v v=%s", ok, v)
	}
}

func TestLRU_HitMissStats(t *testing.T) {
	c := New(2)
	c.Put("a", []byte("1"), time.Minute)
	c.Get("a")       // hit
	c.Get("a")       // hit
	c.Get("missing") // miss
	hits, misses, size := c.Stats()
	if hits != 2 || misses != 1 || size != 1 {
		t.Fatalf("expected hits=2 misses=1 size=1, got %d %d %d", hits, misses, size)
	}
}
