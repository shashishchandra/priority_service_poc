package main

/*
#cgo CFLAGS: -O3 -march=native -ffast-math
#include "scoring.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// PPSMatrix is a flat [numPPS × NumOrders] int16 array allocated on the C heap.
// Allocating on the C heap keeps it outside the Go GC, avoiding stop-the-world
// pressure for the large matrix.
// Score is per-PPS only; BinTag determines eligibility, not score.
type PPSMatrix struct {
	ptr    *C.int16_t
	NumPPS int
}

// NewPPSMatrix allocates a zero-initialised PPSMatrix on the C heap.
// Caller must call Free() when done.
func NewPPSMatrix(numPPS int) *PPSMatrix {
	n := C.size_t(numPPS) * C.size_t(NumOrders) * C.size_t(unsafe.Sizeof(C.int16_t(0)))
	ptr := (*C.int16_t)(C.calloc(1, n))
	if ptr == nil {
		panic("scoring_bridge: C.calloc returned nil — out of memory")
	}
	return &PPSMatrix{ptr: ptr, NumPPS: numPPS}
}

// Free releases the C-heap memory backing the PPSMatrix.
func (m *PPSMatrix) Free() {
	if m.ptr != nil {
		C.free(unsafe.Pointer(m.ptr))
		m.ptr = nil
	}
}

// Row returns a Go []int16 view of row `ppsID` in the matrix without copying.
// The slice is backed by C memory; the caller must not retain it past Free().
func (m *PPSMatrix) Row(ppsID int) []int16 {
	base := uintptr(unsafe.Pointer(m.ptr))
	offset := uintptr(ppsID) * uintptr(NumOrders) * 2 // 2 bytes per int16
	rowPtr := (*int16)(unsafe.Pointer(base + offset))
	return unsafe.Slice(rowPtr, NumOrders)
}

// EvictOrder zeroes the score slot for orderID across all PPS rows via the C kernel.
func (m *PPSMatrix) EvictOrder(orderID uint32) {
	if int(orderID) >= NumOrders || m.ptr == nil {
		return
	}
	C.evict_order_scores(m.ptr, C.int(orderID), C.int(m.NumPPS), C.int(NumOrders))
}

// CUpdateAllScores calls the C kernel to add delta[o] (int32) to every PPS row.
// This is the hot path in Phase 1; auto-vectorised to NEON/AVX2 by the C compiler.
func CUpdateAllScores(m *PPSMatrix, delta []int32) {
	if len(delta) == 0 || m.NumPPS == 0 || m.ptr == nil {
		return
	}
	C.update_all_scores_i16(
		m.ptr,
		(*C.int32_t)(unsafe.Pointer(&delta[0])),
		C.int(m.NumPPS),
		C.int(NumOrders),
	)
}

// CUpdateRowRange calls the C SIMD kernel for a contiguous slice of PPS rows
// [startRow, startRow+numRows). Used by phase1Update to fan Phase 1 work across
// e.numWorkers goroutines — each goroutine processes a disjoint row range.
func CUpdateRowRange(m *PPSMatrix, startRow, numRows int, delta []int32) {
	if numRows <= 0 || len(delta) == 0 || m.ptr == nil {
		return
	}
	// Offset the base pointer to the first row owned by this goroutine.
	rowPtr := (*C.int16_t)(unsafe.Pointer(
		uintptr(unsafe.Pointer(m.ptr)) + uintptr(startRow)*uintptr(NumOrders)*2,
	))
	C.update_all_scores_i16(
		rowPtr,
		(*C.int32_t)(unsafe.Pointer(&delta[0])),
		C.int(numRows),
		C.int(NumOrders),
	)
}

// CInitScores calls the C kernel to initialise all PPS rows from a weight vector.
func CInitScores(m *PPSMatrix, weights []int16) {
	if len(weights) == 0 || m.NumPPS == 0 || m.ptr == nil {
		return
	}
	C.init_scores_i16(
		m.ptr,
		(*C.int16_t)(unsafe.Pointer(&weights[0])),
		C.int(m.NumPPS),
		C.int(NumOrders),
	)
}
