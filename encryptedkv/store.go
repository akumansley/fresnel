package encryptedkv

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
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
	seq       uint64
}

// Item represents a kv pair
type Item struct {
	K []byte
	V []byte
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.(*Item).K, b.(*Item).K)
}

// New returns a new encryptedkv KV
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	treap := gtreap.NewTreap(itemCompare)

	in, ok := config["key"].([]byte)
	if !ok {
		return nil, fmt.Errorf("must provide [32]byte key")
	}

	key := new([32]byte)
	copy(key[:], in[:32])

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	rv := Store{
		treap:     treap,
		readLock:  sync.RWMutex{},
		mo:        mo,
		writeLock: sync.Mutex{},
		db:        db,
		key:       *key,
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
	rv := Reader{t: s.treap}
	s.readLock.RUnlock()
	return &rv, nil
}

// Writer returns a KV writer
func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s}, nil
}

// Compact compacts the underlying store
func (s *Store) Compact() error {
	// TODO
	return nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
