package main

import "fmt"

// gRPC-style status codes used by GetTopK to signal error conditions.
const (
	// GRPCNotFound is returned when the requested (PPS, BinTag) pair is not
	// in the topology at all (or has been removed).
	GRPCNotFound = uint32(5)
	// GRPCUnavailable is returned when the pair exists but is still warming up
	// and has no ranked results yet.
	GRPCUnavailable = uint32(14)
)

// GetTopKRequest describes a client request for ranked order lists.
type GetTopKRequest struct {
	PPSID    uint32
	BinTagID uint32
	K        int
}

// GetTopKResponse contains ranked order IDs for each of the four ranking tables.
// Entries are valid order IDs; -1 marks unused trailing slots.
type GetTopKResponse struct {
	Heuristic []int32
	Large     []int32
	PBT       []int32
	Critical  []int32
}

// ApiError wraps a gRPC status code and a human-readable message.
type ApiError struct {
	Code uint32
	Msg  string
}

// Error implements the error interface.
func (e *ApiError) Error() string {
	return fmt.Sprintf("gRPC status %d: %s", e.Code, e.Msg)
}

// PriorityServer serves GetTopK requests from the pre-computed DoubleBuffer.
// It is safe for concurrent callers on the read path; PairIndex mutations
// (RefreshPairIndex) must be called from a single owner goroutine.
type PriorityServer struct {
	Topology  *TopologyState
	DB        *DoubleBuffer
	PairIndex map[PairKey]int // key → index into ReadBuffer rows
}

// NewPriorityServer constructs a PriorityServer and builds the initial pair index.
func NewPriorityServer(topo *TopologyState, db *DoubleBuffer, pairs []PairInfo) *PriorityServer {
	s := &PriorityServer{
		Topology:  topo,
		DB:        db,
		PairIndex: make(map[PairKey]int, len(pairs)),
	}
	s.RefreshPairIndex(pairs)
	return s
}

// GetTopK looks up and returns the top-K ranked lists for the given (PPS, BinTag) pair.
//
// Error conditions:
//   - *ApiError{Code: GRPCNotFound}   — pair not in topology (or removed)
//   - *ApiError{Code: GRPCUnavailable} — pair is still warming up
func (s *PriorityServer) GetTopK(req *GetTopKRequest) (*GetTopKResponse, error) {
	key := MakePairKey(req.PPSID, req.BinTagID)

	status, ok := s.Topology.Get(key)
	if !ok || status == StatusRemoved {
		return nil, &ApiError{
			Code: GRPCNotFound,
			Msg:  fmt.Sprintf("pair (pps=%d, bintag=%d) not found", req.PPSID, req.BinTagID),
		}
	}
	if status == StatusWarmingUp {
		return nil, &ApiError{
			Code: GRPCUnavailable,
			Msg:  fmt.Sprintf("pair (pps=%d, bintag=%d) is warming up", req.PPSID, req.BinTagID),
		}
	}

	pairIdx, exists := s.PairIndex[key]
	if !exists {
		return nil, &ApiError{
			Code: GRPCNotFound,
			Msg:  fmt.Sprintf("pair (pps=%d, bintag=%d) not in pair index", req.PPSID, req.BinTagID),
		}
	}

	k := req.K
	if k <= 0 || k > TopK {
		k = TopK
	}

	buf := s.DB.Load()
	resp := &GetTopKResponse{
		Heuristic: make([]int32, k),
		Large:     make([]int32, k),
		PBT:       make([]int32, k),
		Critical:  make([]int32, k),
	}
	copy(resp.Heuristic, buf.GetList(pairIdx, TblHeuristic, k))
	copy(resp.Large, buf.GetList(pairIdx, TblLarge, k))
	copy(resp.PBT, buf.GetList(pairIdx, TblPBT, k))
	copy(resp.Critical, buf.GetList(pairIdx, TblCritical, k))

	return resp, nil
}

// RefreshPairIndex rebuilds the pair-key-to-row-index map from the given pairs slice.
// Call this after adding or removing pairs from the ComputeEngine.
func (s *PriorityServer) RefreshPairIndex(pairs []PairInfo) {
	newIndex := make(map[PairKey]int, len(pairs))
	for i, p := range pairs {
		newIndex[p.Key] = i
	}
	s.PairIndex = newIndex
}
