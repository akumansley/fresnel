// Package wave provides a kv store that persists to an encrypted file
// but is read into an in-memory index for use

package kvape

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/registry"
)

// Name is the name of this kvstore impl
const Name = "kvape"

// Store is the exported interface
type Store struct {
	t store.KVStore
}

// Item represents a kv pair
type Item struct {
	k []byte
	v []byte
}

// New returns a new kvape KV
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	treap, err := gtreap.New(mo, config)

	if err != nil {
		return nil, err
	}

	rv := Store{
		t: treap,
	}

	return &rv, nil
}

// Close closes this store
func (s *Store) Close() error {
	// TODO close the file
	return nil
}

// Reader returns a KV reader
func (s *Store) Reader() (store.KVReader, error) {
	// TODO read in the file to the TREAP
	return s.t.Reader()
}

// Writer returns a KV writer
func (s *Store) Writer() (store.KVWriter, error) {
	// TODO intercept writes and persist them to disk first
	return s.t.Writer()
}

func init() {
	registry.RegisterKVStore(Name, New)
}
