package main

import (
	"math/rand"
	"sync"
	"time"
)

// clampI16 clamps an int32 to the int16 range [-32767, 32767].
func clampI16(v int32) int16 {
	if v > 32767 {
		return 32767
	}
	if v < -32767 {
		return -32767
	}
	return int16(v)
}

// ─── ItemDict mock ──────────────────────────────────────────────────────────

// TPIDUpdate describes a single item-type score change in an ItemDict update cycle.
// OldContrib is the score contribution before this cycle; NewContrib is the new value.
type TPIDUpdate struct {
	TPID       uint64
	OldContrib float32
	NewContrib float32
}

// ItemDictUpdate is the payload returned by one call to MockItemDictClient.Fetch().
// It contains a list of TPID score changes for Phase 1 to apply to the ScoreMatrix.
type ItemDictUpdate struct {
	Updates []TPIDUpdate
}

// MockItemDictClient simulates the upstream item-dictionary service that provides
// per-TPID score contributions. Fetch() sleeps 100–200 ms to model network latency.
type MockItemDictClient struct {
	mu         sync.Mutex
	rng        *rand.Rand
	tpidScores map[uint64]float32
	NumTPIDs   uint64
	UpdateFrac float64 // fraction of TPIDs to update per call (0.10 = 10%)
}

// NewMockItemDictClient creates a client with numTPIDs items, each given a random
// initial score contribution in [0, 1).
func NewMockItemDictClient(seed int64, numTPIDs uint64) *MockItemDictClient {
	rng := rand.New(rand.NewSource(seed))
	scores := make(map[uint64]float32, numTPIDs)
	for i := uint64(0); i < numTPIDs; i++ {
		scores[i] = rng.Float32()
	}
	return &MockItemDictClient{
		rng:        rng,
		tpidScores: scores,
		NumTPIDs:   numTPIDs,
		UpdateFrac: 1,
	}
}

// Fetch simulates a round-trip to the item dictionary service.
// It sleeps for 100–200 ms (uniform random) to model network latency, then
// randomly updates UpdateFrac of the tracked TPIDs and returns the diffs.
func (c *MockItemDictClient) Fetch() *ItemDictUpdate {
	c.mu.Lock()
	delayMs := 100 + c.rng.Intn(101) // [100, 200] ms
	c.mu.Unlock()

	time.Sleep(time.Duration(delayMs) * time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()

	numUpdate := int(float64(c.NumTPIDs) * c.UpdateFrac)
	if numUpdate < 1 {
		numUpdate = 1
	}
	updates := make([]TPIDUpdate, 0, numUpdate)
	for i := 0; i < numUpdate; i++ {
		tpid := uint64(c.rng.Int63n(int64(c.NumTPIDs)))
		oldVal := c.tpidScores[tpid]
		newVal := c.rng.Float32()
		c.tpidScores[tpid] = newVal
		updates = append(updates, TPIDUpdate{
			TPID:       tpid,
			OldContrib: oldVal,
			NewContrib: newVal,
		})
	}
	return &ItemDictUpdate{Updates: updates}
}

// ─── Kafka order stream mock ─────────────────────────────────────────────────

// KafkaOrderEvent represents an order creation or update event consumed from Kafka.
type KafkaOrderEvent struct {
	OrderID         uint32
	RequiredQty     float32
	PBTDeadline     int64    // Unix seconds; -1 = no deadline
	TPIDs           []uint64 // item-type IDs required by this order
	EligibleBinTags []uint32 // BinTag IDs at which this order can be fulfilled
	BaseScore       float32  // initial heuristic score assigned at order creation
}

// MockKafkaOrderStream generates synthetic order events for testing.
type MockKafkaOrderStream struct {
	mu         sync.Mutex
	rng        *rand.Rand
	NumOrders  uint32
	NumTPIDs   uint64
	NumBinTags uint32
}

// NewMockKafkaOrderStream creates a mock Kafka order stream.
// numOrders caps the maximum OrderID generated.
// numTPIDs is the universe of item-type IDs.
// numBinTags is the universe of BinTag IDs.
func NewMockKafkaOrderStream(seed int64, numOrders uint32, numTPIDs uint64, numBinTags uint32) *MockKafkaOrderStream {
	return &MockKafkaOrderStream{
		rng:        rand.New(rand.NewSource(seed)),
		NumOrders:  numOrders,
		NumTPIDs:   numTPIDs,
		NumBinTags: numBinTags,
	}
}

// GenerateBatch generates count synthetic KafkaOrderEvents.
// Each event gets:
//   - A random OrderID in [0, NumOrders)
//   - RequiredQty uniform in [1, 30)
//   - PBTDeadline: 50% chance of having one (now + 600..7200 seconds), else -1
//   - 1–5 random TPIDs from the universe
//   - 1–4 random eligible BinTag IDs from the universe
//   - BaseScore in [0, 1)
func (s *MockKafkaOrderStream) GenerateBatch(count int) []KafkaOrderEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()
	events := make([]KafkaOrderEvent, count)
	for i := 0; i < count; i++ {
		orderID := uint32(s.rng.Int63n(int64(s.NumOrders)))
		qty := 1.0 + s.rng.Float32()*29.0

		deadline := int64(-1)
		if s.rng.Float64() < 0.5 {
			deadline = now + 600 + s.rng.Int63n(6601) // 600..7200 seconds from now
		}

		numTPIDs := 1 + s.rng.Intn(5)
		tpids := make([]uint64, numTPIDs)
		for j := 0; j < numTPIDs; j++ {
			tpids[j] = uint64(s.rng.Int63n(int64(s.NumTPIDs)))
		}

		numBT := 1 + s.rng.Intn(4)
		binTags := make([]uint32, numBT)
		for j := 0; j < numBT; j++ {
			binTags[j] = uint32(s.rng.Int63n(int64(s.NumBinTags)))
		}

		events[i] = KafkaOrderEvent{
			OrderID:         orderID,
			RequiredQty:     qty,
			PBTDeadline:     deadline,
			TPIDs:           tpids,
			EligibleBinTags: binTags,
			BaseScore:       s.rng.Float32(),
		}
	}
	return events
}

// Next returns a KafkaOrderEvent for orderID with ALL bintag IDs eligible.
// Use this instead of GenerateBatch for full-1M-order worst-case benchmarks.
func (s *MockKafkaOrderStream) Next(orderID uint32) KafkaOrderEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()
	qty := 1.0 + s.rng.Float32()*29.0

	deadline := int64(-1)
	if s.rng.Float64() < 0.5 {
		deadline = now + 600 + s.rng.Int63n(6601)
	}

	numTPIDs := 1 + s.rng.Intn(5)
	tpids := make([]uint64, numTPIDs)
	for j := 0; j < numTPIDs; j++ {
		tpids[j] = uint64(s.rng.Int63n(int64(s.NumTPIDs)))
	}

	// All bintag IDs eligible — worst-case scenario.
	binTags := make([]uint32, s.NumBinTags)
	for j := uint32(0); j < s.NumBinTags; j++ {
		binTags[j] = j
	}

	return KafkaOrderEvent{
		OrderID:         orderID,
		RequiredQty:     qty,
		PBTDeadline:     deadline,
		TPIDs:           tpids,
		EligibleBinTags: binTags,
		BaseScore:       s.rng.Float32(),
	}
}

// ─── Kafka topology stream mock ──────────────────────────────────────────────

// TopologyEventKind distinguishes add vs remove PPS events.
type TopologyEventKind int

const (
	EvtAddPPS    TopologyEventKind = iota
	EvtRemovePPS TopologyEventKind = iota
)

// KafkaTopologyEvent represents a topology change event consumed from Kafka.
type KafkaTopologyEvent struct {
	Kind      TopologyEventKind
	PPSID     uint32
	BinTagIDs []uint32 // populated for EvtAddPPS; empty for EvtRemovePPS
}

// MockKafkaTopologyStream generates synthetic topology events for testing.
type MockKafkaTopologyStream struct {
	mu  sync.Mutex
	rng *rand.Rand
}

// NewMockKafkaTopologyStream creates a mock topology event stream.
func NewMockKafkaTopologyStream(seed int64) *MockKafkaTopologyStream {
	return &MockKafkaTopologyStream{
		rng: rand.New(rand.NewSource(seed)),
	}
}

// AddPPSEvent creates a synthetic EvtAddPPS event for ppsID with numBinTags BinTag IDs.
// BinTag IDs are sequential starting from a random offset.
func (s *MockKafkaTopologyStream) AddPPSEvent(ppsID uint32, numBinTags int) KafkaTopologyEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := uint32(s.rng.Int63n(1000))
	binTags := make([]uint32, numBinTags)
	for i := 0; i < numBinTags; i++ {
		binTags[i] = offset + uint32(i)
	}
	return KafkaTopologyEvent{
		Kind:      EvtAddPPS,
		PPSID:     ppsID,
		BinTagIDs: binTags,
	}
}

// RemovePPSEvent creates a synthetic EvtRemovePPS event for ppsID.
func (s *MockKafkaTopologyStream) RemovePPSEvent(ppsID uint32) KafkaTopologyEvent {
	return KafkaTopologyEvent{
		Kind:  EvtRemovePPS,
		PPSID: ppsID,
	}
}

// ─── Order ingestion ─────────────────────────────────────────────────────────

// IngestOrder applies a KafkaOrderEvent to the registry, inverted indexes, and PPSMatrix.
// The base score (scaled to int16) is written into every PPS row for this order.
// BinTag determines eligibility only — score is per-PPS, not per-BinTag.
//
// This is called once per order event from the Kafka consumer loop. It is not
// goroutine-safe with respect to the registry and indexes; callers must serialise
// ingestion (e.g., single consumer goroutine) or add their own locking.
func IngestOrder(evt KafkaOrderEvent, registry *OrderRegistry, indexes *InvertedIndexes, matrix *PPSMatrix, nowSecs int64) {
	meta := OrderMeta{
		OrderID:        evt.OrderID,
		RequiredQty:    evt.RequiredQty,
		PBTDeadline:    evt.PBTDeadline,
		Active:         true,
		InsertedAtSecs: nowSecs,
	}
	// Remove old index entries before upserting to avoid duplicate entries.
	if registry.Meta[evt.OrderID].Active {
		indexes.RemoveOrder(evt.OrderID)
	}
	registry.Upsert(meta)
	indexes.AddOrder(evt.OrderID, evt.TPIDs, evt.EligibleBinTags)

	// Write base score (scaled to int16) into every PPS row for this order.
	if matrix != nil && evt.OrderID < uint32(NumOrders) {
		baseI16 := clampI16(int32(evt.BaseScore * ScoreScale))
		oid := int(evt.OrderID)
		for p := 0; p < matrix.NumPPS; p++ {
			row := matrix.Row(p)
			row[oid] = baseI16
		}
	}
}
