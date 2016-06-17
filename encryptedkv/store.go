package encryptedkv

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	gtreapBleve "github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/registry"
	"github.com/steveyen/gtreap"
	"github.com/syndtr/goleveldb/leveldb"
)

// Name is the name of this kvstore impl
const Name = "encryptedkv"

// Store is the exported interface
type Store struct {
	readLock  sync.RWMutex
	treap     *gtreap.Treap
	mo        store.MergeOperator
	writeLock sync.Mutex
	db        *leveldb.DB
	key       [32]byte
	seq       int
}

// Item represents a kv pair
type Item struct {
	k []byte
	v []byte
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.(*Item).k, b.(*Item).k)
}

// New returns a new encryptedkv KV
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	treap := gtreap.NewTreap(itemCompare)

	key, ok := config["key"].([32]byte)
	if !ok {
		return nil, fmt.Errorf("must provide [32]byte key")
	}

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	opts := opts.Options{} // TODO what to do what to do

	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	rv := Store{
		treap:     treap,
		readLock:  sync.RWMutex{},
		mo:        mo,
		writeLock: sync.Mutex{},
		db:        db,
		key:       key,
	}

	err = rv.loadFromFile()
	if err != nil {
		return nil, err
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
	s.readLock.RLock()
	rv := gtreapBleve.Reader{t: s.treap}
	s.readLock.RUnlock()
	return rv
}

// Writer returns a KV writer
func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s}
}

// Compact compacts the underlying store
func (s *Store) Compact() error {
	// TODO
	return nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
