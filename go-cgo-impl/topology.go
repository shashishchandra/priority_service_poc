package main

import (
	"sync"
	"sync/atomic"
)

// PairStatus represents the lifecycle state of a (PPS, BinTag) pair.
type PairStatus int

const (
	// StatusActive means the pair has completed warm-up and serves live traffic.
	StatusActive PairStatus = iota
	// StatusWarmingUp means the pair was recently added; its scores are being
	// initialised and it must not serve read traffic yet.
	StatusWarmingUp
	// StatusRemoved means the pair has been decommissioned and is no longer valid.
	StatusRemoved
)

// TopologyState tracks the lifecycle state of all (PPS, BinTag) pairs in this pod.
// Reads use atomic.Value (copy-on-write map) so they are lock-free and wait-free.
// Writes acquire mu, copy the map, mutate the copy, and atomically publish the new map.
type TopologyState struct {
	val atomic.Value // stores map[PairKey]PairStatus
	mu  sync.Mutex  // held only during writes to serialise COW copies
}

// NewTopologyState creates a TopologyState with an empty pair map.
func NewTopologyState() *TopologyState {
	t := &TopologyState{}
	t.val.Store(make(map[PairKey]PairStatus))
	return t
}

// loadMap returns the current map snapshot. Safe for concurrent reads.
func (t *TopologyState) loadMap() map[PairKey]PairStatus {
	return t.val.Load().(map[PairKey]PairStatus)
}

// copyMap creates a shallow copy of the current map for COW mutation.
// Must be called with mu held.
func (t *TopologyState) copyMap() map[PairKey]PairStatus {
	src := t.loadMap()
	dst := make(map[PairKey]PairStatus, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Get returns the status for the pair identified by key.
// Returns (status, true) if found, or (0, false) if the pair is unknown.
func (t *TopologyState) Get(key PairKey) (PairStatus, bool) {
	m := t.loadMap()
	s, ok := m[key]
	return s, ok
}

// AddWarmingUp registers a new pair as StatusWarmingUp.
// The pair will be promoted to StatusActive after its first successful compute cycle.
func (t *TopologyState) AddWarmingUp(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.copyMap()
	m[key] = StatusWarmingUp
	t.val.Store(m)
}

// Activate promotes a pair from StatusWarmingUp to StatusActive.
// Safe to call even if the pair is already Active (idempotent).
func (t *TopologyState) Activate(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.copyMap()
	m[key] = StatusActive
	t.val.Store(m)
}

// Remove marks a pair as StatusRemoved, preventing further read traffic.
func (t *TopologyState) Remove(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.copyMap()
	m[key] = StatusRemoved
	t.val.Store(m)
}

// WarmingUpPairs returns a snapshot of all pair keys currently in StatusWarmingUp.
// Used by ComputeEngine.promoteWarmingPairs to know which pairs to promote after a cycle.
func (t *TopologyState) WarmingUpPairs() []PairKey {
	m := t.loadMap()
	var result []PairKey
	for k, s := range m {
		if s == StatusWarmingUp {
			result = append(result, k)
		}
	}
	return result
}
