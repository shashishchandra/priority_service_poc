package main

import (
	"sync"
	"sync/atomic"
)

// PairStatus describes the lifecycle state of a (PPS, BinTag) pair.
type PairStatus int

const (
	// StatusActive means the pair is fully ranked and results are served.
	StatusActive PairStatus = iota
	// StatusWarmingUp means the pair has been added but has not yet completed
	// its first full compute cycle; GetTopK returns Unavailable for it.
	StatusWarmingUp
	// StatusRemoved means the pair has been removed from the topology;
	// GetTopK returns NotFound for it.
	StatusRemoved
)

// topologyMap is the immutable snapshot stored inside atomic.Value.
type topologyMap map[PairKey]PairStatus

// TopologyState manages pair lifecycle with COW (copy-on-write) semantics.
// Reads are lock-free via atomic.Value.Load(); writes take a mutex to serialise
// the copy-and-swap.
type TopologyState struct {
	val atomic.Value // stores topologyMap
	mu  sync.Mutex
}

// NewTopologyState returns a TopologyState with no pairs registered.
func NewTopologyState() *TopologyState {
	ts := &TopologyState{}
	ts.val.Store(make(topologyMap))
	return ts
}

// Get looks up the status of the pair identified by key.
// Returns (status, true) if found; (0, false) if not present.
// Lock-free.
func (t *TopologyState) Get(key PairKey) (PairStatus, bool) {
	m := t.val.Load().(topologyMap)
	status, ok := m[key]
	return status, ok
}

// AddWarmingUp registers a new pair in StatusWarmingUp state.
func (t *TopologyState) AddWarmingUp(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	old := t.val.Load().(topologyMap)
	next := copyMap(old)
	next[key] = StatusWarmingUp
	t.val.Store(next)
}

// Activate transitions a pair from StatusWarmingUp to StatusActive.
// If the pair is not in StatusWarmingUp this is a no-op.
func (t *TopologyState) Activate(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	old := t.val.Load().(topologyMap)
	if old[key] != StatusWarmingUp {
		return
	}
	next := copyMap(old)
	next[key] = StatusActive
	t.val.Store(next)
}

// Remove transitions a pair to StatusRemoved (or inserts it as removed if
// it was not previously known).
func (t *TopologyState) Remove(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	old := t.val.Load().(topologyMap)
	next := copyMap(old)
	next[key] = StatusRemoved
	t.val.Store(next)
}

// WarmingUpPairs returns the keys of all pairs currently in StatusWarmingUp.
// Lock-free snapshot read.
func (t *TopologyState) WarmingUpPairs() []PairKey {
	m := t.val.Load().(topologyMap)
	var out []PairKey
	for k, s := range m {
		if s == StatusWarmingUp {
			out = append(out, k)
		}
	}
	return out
}

// copyMap returns a shallow copy of a topologyMap.
func copyMap(src topologyMap) topologyMap {
	dst := make(topologyMap, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
