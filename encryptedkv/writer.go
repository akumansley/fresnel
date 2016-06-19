package encryptedkv

import (
	"fmt"
	"math/rand"

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
	var items []Item

	t := w.s.treap
	// do work here
	w.s.writeLock.Lock()
	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)
		var existingVal []byte
		existingItem := t.Get(&Item{K: kb})
		if existingItem != nil {
			existingVal = t.Get(&Item{K: kb}).(*Item).V  // why is this necessary
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		items = append(items, Item{K: kb, V: mergedVal})
		t = t.Upsert(&Item{K: kb, V: mergedVal}, rand.Int())
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			items = append(items, Item{K: op.K, V: op.V})
			t = t.Upsert(&Item{K: op.K, V: op.V}, rand.Int())
		} else {
			items = append(items, Item{K: op.K, V: nil})
			t = t.Delete(&Item{K: op.K})
		}
	}

	writeBatchToFile(w.s, items)
	w.s.readLock.Lock()
	w.s.treap = t
	w.s.readLock.Unlock()
	w.s.writeLock.Unlock()

	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	w.s = nil
	return nil
}
