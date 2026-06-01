package main

// OrderRegistry holds flat per-order metadata indexed by OrderID.
type OrderRegistry struct {
	Meta [NumOrders]OrderMeta
}

func NewOrderRegistry() *OrderRegistry { return &OrderRegistry{} }

func (r *OrderRegistry) Upsert(m OrderMeta) {
	r.Meta[m.OrderID] = m
}

func (r *OrderRegistry) Deactivate(orderID uint32) {
	r.Meta[orderID].Active = false
}

// EvictOrder deactivates an order and cleans up its index entries and PPSMatrix slots.
// Call this on Kafka order-removed events.
func (r *OrderRegistry) EvictOrder(orderID uint32, indexes *InvertedIndexes, matrix *PPSMatrix) {
	if int(orderID) >= NumOrders || !r.Meta[orderID].Active {
		return
	}
	r.Meta[orderID].Active = false
	r.Meta[orderID].InsertedAtSecs = 0
	indexes.RemoveOrder(orderID)
	matrix.EvictOrder(orderID)
}

// PurgeExpired scans all orders and evicts those whose TTL has expired.
// Call at the start of each compute cycle.
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
type InvertedIndexes struct {
	TPIDToOrders   map[uint64][]uint32
	BinTagToOrders map[uint32][]uint32
	reverseTPID    map[uint32][]uint64
	reverseBinTag  map[uint32][]uint32
}

func NewInvertedIndexes() *InvertedIndexes {
	return &InvertedIndexes{
		TPIDToOrders:   make(map[uint64][]uint32),
		BinTagToOrders: make(map[uint32][]uint32),
		reverseTPID:    make(map[uint32][]uint64),
		reverseBinTag:  make(map[uint32][]uint32),
	}
}

func (ix *InvertedIndexes) AddOrder(orderID uint32, tpids []uint64, bintagIDs []uint32) {
	ix.RemoveOrder(orderID)

	for _, tpid := range tpids {
		ix.TPIDToOrders[tpid] = append(ix.TPIDToOrders[tpid], orderID)
	}
	for _, btID := range bintagIDs {
		ix.BinTagToOrders[btID] = append(ix.BinTagToOrders[btID], orderID)
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

func removeUint32(s []uint32, v uint32) []uint32 {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}
