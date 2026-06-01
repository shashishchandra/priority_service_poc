package main

import (
	"math/rand"
	"time"
)

// ---------------------------------------------------------------------------
// MockItemDictClient — simulates an item-dictionary gRPC client
// ---------------------------------------------------------------------------

// TPIDDelta represents a score delta for a single TPID from the item dictionary.
type TPIDDelta struct {
	TPID       uint64
	ScoreDelta float32
}

// ItemDictUpdate carries a batch of per-TPID score deltas for Phase 1.
type ItemDictUpdate struct {
	TPIDDeltas []TPIDDelta
	Timestamp  int64
}

// MockItemDictClient simulates slow item-dictionary network calls.
// It produces a random ItemDictUpdate whose latency is drawn from a uniform
// [minLatency, maxLatency] range so that tests can exercise SLA boundaries.
type MockItemDictClient struct {
	rng        *rand.Rand
	minLatency time.Duration
	maxLatency time.Duration
	numTPIDs   int
}

// NewMockItemDictClient returns a client with realistic default parameters.
func NewMockItemDictClient() *MockItemDictClient {
	return &MockItemDictClient{
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		minLatency: 10 * time.Millisecond,
		maxLatency: 80 * time.Millisecond,
		numTPIDs:   100000,
	}
}

// FetchUpdate simulates a network round-trip and returns a synthetic
// ItemDictUpdate containing numTPIDs entries with random score deltas.
func (c *MockItemDictClient) FetchUpdate() *ItemDictUpdate {
	spread := int64(c.maxLatency - c.minLatency)
	lat := c.minLatency + time.Duration(c.rng.Int63n(spread+1))
	time.Sleep(lat)

	deltas := make([]TPIDDelta, c.numTPIDs)
	for i := range deltas {
		deltas[i] = TPIDDelta{
			TPID:       uint64(c.rng.Intn(100_000)),
			ScoreDelta: c.rng.Float32()*0.2 - 0.1, // [-0.1, 0.1)
		}
	}
	return &ItemDictUpdate{TPIDDeltas: deltas, Timestamp: time.Now().Unix()}
}

// SetLatency overrides the simulated latency range — useful in tests.
func (c *MockItemDictClient) SetLatency(min, max time.Duration) {
	c.minLatency = min
	c.maxLatency = max
}

// ---------------------------------------------------------------------------
// KafkaOrderEvent — represents a single order event from Kafka
// ---------------------------------------------------------------------------

// KafkaOrderEvent is the payload produced by the order ingest Kafka topic.
type KafkaOrderEvent struct {
	OrderID         uint32
	RequiredQty     float32
	PBTDeadline     int64
	BaseScore       float32
	TPIDs           []uint64
	EligibleBinTags []uint32
	Active          bool
}

// IngestOrder applies a KafkaOrderEvent to the registry, inverted indexes, and
// PPSMatrix. The base score (scaled to int16) is written into every PPS row
// for this order. BinTag determines eligibility only, not score.
func IngestOrder(evt KafkaOrderEvent, registry *OrderRegistry, indexes *InvertedIndexes, matrix *PPSMatrix, nowSecs int64) {
	meta := OrderMeta{
		OrderID:        evt.OrderID,
		RequiredQty:    evt.RequiredQty,
		PBTDeadline:    evt.PBTDeadline,
		Active:         evt.Active,
		InsertedAtSecs: nowSecs,
	}
	registry.Upsert(meta)
	indexes.AddOrder(evt.OrderID, evt.TPIDs, evt.EligibleBinTags)

	// Write base score to every PPS row. Score is per-PPS (BinTag is eligibility only).
	baseI16 := clampInt16(int32(evt.BaseScore * ScoreScale))
	oid := int(evt.OrderID)
	for p := 0; p < matrix.NumPPS; p++ {
		matrix.Data[p*NumOrders+oid] = baseI16
	}
}

// ---------------------------------------------------------------------------
// MockKafkaOrderStream — simulates a Kafka consumer for order events
// ---------------------------------------------------------------------------

// MockKafkaOrderStream generates synthetic KafkaOrderEvents for benchmarking
// and integration testing.
type MockKafkaOrderStream struct {
	rng      *rand.Rand
	numPairs int
}

// NewMockKafkaOrderStream creates a stream that will reference up to numPairs
// bintag IDs so that inverted-index lookups are exercised.
func NewMockKafkaOrderStream(numPairs int) *MockKafkaOrderStream {
	return &MockKafkaOrderStream{
		rng:      rand.New(rand.NewSource(12345)),
		numPairs: numPairs,
	}
}

// Next returns a KafkaOrderEvent for orderID with ALL bintag IDs eligible.
// go-pure uses 1-indexed bintag IDs (1..numBinTags) to match buildPairs.
func (s *MockKafkaOrderStream) Next(orderID uint32) KafkaOrderEvent {
	// All bintag IDs eligible — worst-case scenario.
	binTags := make([]uint32, numBinTags)
	for i := uint32(0); i < uint32(numBinTags); i++ {
		binTags[i] = i + 1 // 1-indexed: 1..numBinTags
	}

	numTPIDs := 1 + s.rng.Intn(10)
	tpids := make([]uint64, numTPIDs)
	for i := range tpids {
		tpids[i] = uint64(s.rng.Intn(100_000))
	}

	return KafkaOrderEvent{
		OrderID:         orderID,
		RequiredQty:     s.rng.Float32() * 50,
		PBTDeadline:     time.Now().Unix() + int64(s.rng.Intn(7200)),
		BaseScore:       s.rng.Float32(),
		TPIDs:           tpids,
		EligibleBinTags: binTags,
		Active:          true,
	}
}

// ---------------------------------------------------------------------------
// MockKafkaTopologyStream — simulates topology change events
// ---------------------------------------------------------------------------

// TopologyEventKind distinguishes add vs remove topology events.
type TopologyEventKind int

const (
	TopologyAdd    TopologyEventKind = iota
	TopologyRemove                   = iota
)

// TopologyEvent represents a single topology change (pair added or removed).
type TopologyEvent struct {
	Kind     TopologyEventKind
	PPSID    uint32
	BinTagID uint32
}

// MockKafkaTopologyStream generates synthetic topology events.
type MockKafkaTopologyStream struct {
	rng *rand.Rand
}

// NewMockKafkaTopologyStream creates a new stream.
func NewMockKafkaTopologyStream() *MockKafkaTopologyStream {
	return &MockKafkaTopologyStream{rng: rand.New(rand.NewSource(99))}
}

// NextAdd returns a random add event for a new (PPS, BinTag) pair.
func (s *MockKafkaTopologyStream) NextAdd(ppsID, bintagID uint32) TopologyEvent {
	return TopologyEvent{Kind: TopologyAdd, PPSID: ppsID, BinTagID: bintagID}
}

// NextRemove returns a remove event for the given pair.
func (s *MockKafkaTopologyStream) NextRemove(ppsID, bintagID uint32) TopologyEvent {
	return TopologyEvent{Kind: TopologyRemove, PPSID: ppsID, BinTagID: bintagID}
}
