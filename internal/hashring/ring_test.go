package hashring

import (
	"fmt"
	"math"
	"testing"
)

func TestBasic_AddGet(t *testing.T) {
	r := New(64)
	r.Add("redis-0")
	r.Add("redis-1")
	r.Add("redis-2")

	if r.Size() != 3 {
		t.Fatalf("expected 3 nodes, got %d", r.Size())
	}
	node := r.Get("user_42")
	if node == "" {
		t.Fatal("expected a node, got empty")
	}
	t.Logf("user_42 → %s", node)
}

func TestEmptyRing(t *testing.T) {
	r := New(64)
	if got := r.Get("anything"); got != "" {
		t.Fatalf("expected empty string for empty ring, got %q", got)
	}
}

func TestDeterministic(t *testing.T) {
	// Same key + same nodes → same answer every time.
	r := New(64)
	r.Add("redis-0")
	r.Add("redis-1")
	r.Add("redis-2")

	first := r.Get("user_42")
	for i := 0; i < 1000; i++ {
		if got := r.Get("user_42"); got != first {
			t.Fatalf("nondeterministic: iteration %d returned %s, expected %s", i, got, first)
		}
	}
}

func TestAddDuplicate(t *testing.T) {
	r := New(64)
	r.Add("redis-0")
	r.Add("redis-0") // duplicate

	if r.Size() != 1 {
		t.Fatalf("duplicate Add should be no-op, got size %d", r.Size())
	}
}

func TestRemoveNonexistent(t *testing.T) {
	r := New(64)
	r.Add("redis-0")
	r.Remove("ghost") // should not panic
	if r.Size() != 1 {
		t.Fatalf("Remove of nonexistent node changed size: got %d", r.Size())
	}
}

// TestDistribution checks that 10000 keys are spread roughly evenly
// across 5 nodes. With 150 virtual nodes per real node, each real node
// should get within 25% of the mean.
func TestDistribution(t *testing.T) {
	r := New(150)
	for i := 0; i < 5; i++ {
		r.Add(fmt.Sprintf("redis-%d", i))
	}

	counts := make(map[string]int)
	const N = 10000
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("user_%d", i)
		counts[r.Get(k)]++
	}

	mean := float64(N) / 5.0
	tolerance := 0.25 // ±25% of the mean
	for node, c := range counts {
		dev := math.Abs(float64(c)-mean) / mean
		t.Logf("%s: %d keys (%.2f%% off mean)", node, c, dev*100)
		if dev > tolerance {
			t.Fatalf("%s deviates %.2f%% from mean — distribution too skewed", node, dev*100)
		}
	}
}

// TestConsistency_AddNode is the headline test. The property we care about:
// when we add a new node, only ~1/N of keys should remap.
func TestConsistency_AddNode(t *testing.T) {
	r := New(150)
	for i := 0; i < 4; i++ {
		r.Add(fmt.Sprintf("redis-%d", i))
	}

	const N = 10000
	keys := make([]string, N)
	before := make(map[string]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("user_%d", i)
		keys[i] = k
		before[k] = r.Get(k)
	}

	// Add a 5th node. Math expectation: ~1/5 = 20% of keys remap.
	r.Add("redis-4")

	remapped := 0
	for _, k := range keys {
		if r.Get(k) != before[k] {
			remapped++
		}
	}
	pct := float64(remapped) / float64(N) * 100
	t.Logf("After adding node 5: %.2f%% of keys remapped (theory: ~20%%)", pct)

	// Should be in the 15-30% range. Wider window allows for variance.
	if pct < 10 || pct > 35 {
		t.Fatalf("remap percentage %.2f%% outside expected range 10-35%%", pct)
	}
}

// TestConsistency_RemoveNode — same property when removing a node.
func TestConsistency_RemoveNode(t *testing.T) {
	r := New(150)
	for i := 0; i < 5; i++ {
		r.Add(fmt.Sprintf("redis-%d", i))
	}

	const N = 10000
	keys := make([]string, N)
	before := make(map[string]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("user_%d", i)
		keys[i] = k
		before[k] = r.Get(k)
	}

	// Remove a node. Theory: 1/5 = 20% of keys remap (those that lived on
	// the removed node need a new home).
	r.Remove("redis-2")

	remapped := 0
	for _, k := range keys {
		if r.Get(k) != before[k] {
			remapped++
		}
	}
	pct := float64(remapped) / float64(N) * 100
	t.Logf("After removing 1 of 5 nodes: %.2f%% of keys remapped (theory: ~20%%)", pct)

	if pct < 10 || pct > 35 {
		t.Fatalf("remap percentage %.2f%% outside expected range 10-35%%", pct)
	}
}
