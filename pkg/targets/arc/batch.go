package arc

import (
	"github.com/timescale/tsbs/pkg/data"
)

// batch holds pre-computed columnar batches ready to send
// With pre-generated columnar data, each "batch" from TSBS is actually
// one pre-computed columnar batch that's ready to send directly
type batch struct {
	batches  []*columnarBatch // Pre-computed batches to send
	metrics  uint64
	rowCount uint64
}

// Len returns the number of items in the batch
func (b *batch) Len() uint {
	return uint(len(b.batches))
}

// Append adds a pre-computed columnar batch
func (b *batch) Append(item data.LoadedPoint) {
	cb := item.Data.(*columnarBatch)
	if cb == nil {
		return
	}

	b.batches = append(b.batches, cb)
	b.rowCount += uint64(cb.rowCount)
	// Estimate metrics (10 fields per row for cpu-only use case)
	b.metrics += uint64(cb.rowCount) * 10
}

// GetBatches returns all pre-computed batches for sending
func (b *batch) GetBatches() []*columnarBatch {
	return b.batches
}

// Reset clears the batch for reuse
func (b *batch) Reset() {
	b.batches = b.batches[:0]
	b.metrics = 0
	b.rowCount = 0
}
