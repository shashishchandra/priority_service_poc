package main

// OrderRegistry holds flat per-order metadata indexed directly by OrderID.
// Access is O(1) by array index, avoiding map overhead on the hot read path.
type OrderRegistry struct {
	Meta [NumOrders]OrderMeta // indexed by order_id; order_id must be < NumOrders
}

// NewOrderRegistry allocates and returns an empty OrderRegistry.
// All slots are zero-valued (Active=false, PBTDeadline=0).
func NewOrderRegistry() *OrderRegistry {
	return &OrderRegistry{}
}

// Upsert inserts or updates the metadata for an order.
// The order's ID must be < NumOrders; orders outside this range are silently dropped.
func (r *OrderRegistry) Upsert(m OrderMeta) {
	if m.OrderID >= NumOrders {
		return
	}
	r.Meta[m.OrderID] = m
}

// Deactivate marks the order as inactive so it is excluded from future ranking cycles.
func (r *OrderRegistry) Deactivate(orderID uint32) {
	if orderID >= NumOrders {
		return
	}
	r.Meta[orderID].Active = false
}

// EvictOrder deactivates an order and cleans up its index entries and PPSMatrix slots.
// Use this for Kafka order-removed events.
func (r *OrderRegistry) EvictOrder(orderID uint32, indexes *InvertedIndexes, matrix *PPSMatrix) {
	if orderID >= NumOrders || !r.Meta[orderID].Active {
		return
	}
	r.Meta[orderID].Active = false
	r.Meta[orderID].InsertedAtSecs = 0
	indexes.RemoveOrder(orderID)
	matrix.EvictOrder(orderID)
}

// PurgeExpired scans all orders and evicts those whose TTL has expired.
func (r *OrderRegistry) PurgeExpired(nowSecs int64, indexes *InvertedIndexes, matrix *PPSMatrix) {
	for i := 0; i < NumOrders; i++ {
		m := &r.Meta[i]
		if m.Active && m.InsertedAtSecs > 0 && (nowSecs-m.InsertedAtSecs) > OrderTTLSecs {
			orderID := m.OrderID
			m.Active = false
			m.InsertedAtSecs = 0
			indexes.RemoveOrder(orderID)
			matrix.EvictOrder(orderID)
		}
	}
}

// InvertedIndexes maps TPID and BinTag IDs to sets of order IDs.
// Includes reverse maps for O(1) per-order cleanup during eviction.
type InvertedIndexes struct {
	TPIDToOrders   map[uint64][]uint32 // TPID → order IDs
	BinTagToOrders map[uint32][]uint32 // BinTag → eligible order IDs
	reverseTPID    map[uint32][]uint64 // orderID → TPIDs it's registered under
	reverseBinTag  map[uint32][]uint32 // orderID → BinTag IDs it's registered under
}

// NewInvertedIndexes allocates and returns empty inverted indexes.
func NewInvertedIndexes() *InvertedIndexes {
	return &InvertedIndexes{
		TPIDToOrders:   make(map[uint64][]uint32),
		BinTagToOrders: make(map[uint32][]uint32),
		reverseTPID:    make(map[uint32][]uint64),
		reverseBinTag:  make(map[uint32][]uint32),
	}
}

// AddOrder registers orderID under all given TPIDs and BinTag IDs (idempotent upsert).
// Callers should call RemoveOrder before re-adding if an order is being updated.
func (ix *InvertedIndexes) AddOrder(orderID uint32, tpids []uint64, bintagIDs []uint32) {
	ix.RemoveOrder(orderID) // idempotent: remove stale entries first

	for _, t := range tpids {
		ix.TPIDToOrders[t] = append(ix.TPIDToOrders[t], orderID)
	}
	for _, b := range bintagIDs {
		ix.BinTagToOrders[b] = append(ix.BinTagToOrders[b], orderID)
	}

	if len(tpids) > 0 {
		cp := make([]uint64, len(tpids))
		copy(cp, tpids)
		ix.reverseTPID[orderID] = cp
	}
	if len(bintagIDs) > 0 {
		cp := make([]uint32, len(bintagIDs))
		copy(cp, bintagIDs)
		ix.reverseBinTag[orderID] = cp
	}
}

// RemoveOrder removes all index entries for orderID using the reverse maps.
func (ix *InvertedIndexes) RemoveOrder(orderID uint32) {
	for _, tpid := range ix.reverseTPID[orderID] {
		list := ix.TPIDToOrders[tpid]
		list = removeUint32(list, orderID)
		if len(list) == 0 {
			delete(ix.TPIDToOrders, tpid)
		} else {
			ix.TPIDToOrders[tpid] = list
		}
	}
	delete(ix.reverseTPID, orderID)

	for _, btID := range ix.reverseBinTag[orderID] {
		list := ix.BinTagToOrders[btID]
		list = removeUint32(list, orderID)
		if len(list) == 0 {
			delete(ix.BinTagToOrders, btID)
		} else {
			ix.BinTagToOrders[btID] = list
		}
	}
	delete(ix.reverseBinTag, orderID)
}

// removeUint32 removes the first occurrence of v from s (swap-and-shrink).
func removeUint32(s []uint32, v uint32) []uint32 {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}
