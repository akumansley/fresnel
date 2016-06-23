package ekv

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
)

// Writer writes to a store
type Writer struct {
	s *Store
}

// NewBatch returns a new batch
func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.s.mo)
}

// NewBatchEx returns a new batch
func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

// ExecuteBatch executes a batch write
func (w *Writer) ExecuteBatch(batch store.KVBatch) error {
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)
		var existingVal []byte
		existingVal, err := w.s.c.Get(kb)
		if err != nil {
			return err
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		err = w.s.c.Set(kb, mergedVal)
		if err != nil {
			return err
		}
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			w.s.c.Set(op.K, op.V)
		} else {
			w.s.c.Delete(op.K)
		}
	}

	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	w.s = nil
	return nil
}
