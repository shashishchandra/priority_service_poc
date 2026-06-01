package main

import "fmt"

// gRPC canonical status codes used by the circuit breaker.
const (
	GRPCNotFound    = uint32(5)
	GRPCUnavailable = uint32(14)
)

// GetTopKRequest carries the parameters for a GetTopK RPC call.
type GetTopKRequest struct {
	PPSID    uint32
	BinTagID uint32
	K        int
}

// GetTopKResponse holds the ranked order lists for all four tables.
type GetTopKResponse struct {
	Heuristic []int32
	Large     []int32
	PBT       []int32
	Critical  []int32
}

// ApiError is a structured error returned by the server when a request cannot be served.
// Code maps to a gRPC status code so callers can decide on retry behaviour.
type ApiError struct {
	Code uint32
	Msg  string
}

// Error implements the error interface.
func (e *ApiError) Error() string {
	return fmt.Sprintf("ApiError(code=%d): %s", e.Code, e.Msg)
}

// PriorityServer exposes the priority-ranking data to callers.
// It is read-heavy and lock-free on the hot path: topology checks use atomic.Value
// snapshots, and the ReadBuffer is accessed via atomic.Pointer.
type PriorityServer struct {
	Topology  *TopologyState
	DB        *DoubleBuffer
	PairIndex map[PairKey]int // PairKey → index into ReadBuffer rows
}

// NewPriorityServer creates a PriorityServer and builds the initial PairIndex.
func NewPriorityServer(topo *TopologyState, db *DoubleBuffer, pairs []PairInfo) *PriorityServer {
	s := &PriorityServer{
		Topology:  topo,
		DB:        db,
		PairIndex: make(map[PairKey]int, len(pairs)),
	}
	for i, p := range pairs {
		s.PairIndex[p.Key] = i
	}
	return s
}

// GetTopK implements the circuit-breaker pattern:
//
//  1. If the pair is not found in TopologyState → GRPCNotFound (5).
//  2. If the pair is WarmingUp → GRPCUnavailable (14).
//  3. If the pair is Active → load the current ReadBuffer and return the lists.
//
// The hot path (Active) is lock-free: it reads topology via atomic.Value and the
// ReadBuffer via atomic.Pointer.
func (s *PriorityServer) GetTopK(req *GetTopKRequest) (*GetTopKResponse, error) {
	key := MakePairKey(req.PPSID, req.BinTagID)

	status, found := s.Topology.Get(key)
	if !found || status == StatusRemoved {
		return nil, &ApiError{Code: GRPCNotFound, Msg: "pair not found"}
	}
	if status == StatusWarmingUp {
		return nil, &ApiError{Code: GRPCUnavailable, Msg: "pair warming up"}
	}

	pairIdx, ok := s.PairIndex[key]
	if !ok {
		// Pair is Active in topology but not yet in our index — treat as unavailable.
		return nil, &ApiError{Code: GRPCUnavailable, Msg: "pair index not refreshed yet"}
	}

	buf := s.DB.Load()
	k := req.K
	if k <= 0 || k > TopK {
		k = TopK
	}

	return &GetTopKResponse{
		Heuristic: buf.GetList(pairIdx, TblHeuristic, k),
		Large:     buf.GetList(pairIdx, TblLarge, k),
		PBT:       buf.GetList(pairIdx, TblPBT, k),
		Critical:  buf.GetList(pairIdx, TblCritical, k),
	}, nil
}

// RefreshPairIndex rebuilds the PairKey → ReadBuffer row index after pairs are added
// or removed. Must be called after HandleAddPair / HandleRemovePair and after the
// matching ReadBuffer has been published.
func (s *PriorityServer) RefreshPairIndex(pairs []PairInfo) {
	newIdx := make(map[PairKey]int, len(pairs))
	for i, p := range pairs {
		newIdx[p.Key] = i
	}
	s.PairIndex = newIdx
}
