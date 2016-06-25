package ekv

import (
	"fmt"

	"github.com/awans/fresnel/encryptedfile"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/steveyen/gkvlite"
)

// Name is the name of this kvstore impl
const Name = "ekv"
const collectionName = "bleve"

// Store is the exported interface
type Store struct {
	mo store.MergeOperator
	c  *gkvlite.Collection
	s  *gkvlite.Store
	ef *encryptedfile.EncryptedFile
}

// New returns a new encryptedkv KV
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
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
	f, err := encryptedfile.Open(path, *key)
	if err != nil {
		return nil, err
	}

	s, err := gkvlite.NewStore(f)
	if err != nil {
		return nil, err
	}
	c := s.SetCollection(collectionName, nil)

	rv := Store{
		mo: mo,
		c:  c,
		s:  s,
		ef: f,
	}

	return &rv, nil
}

// Close closes this store
func (s *Store) Close() error {
	s.s.Flush()
	s.s.Close()
	s.ef.Sync()
	return nil
}

// Reader returns a KV reader
func (s *Store) Reader() (store.KVReader, error) {
	snap := s.s.Snapshot()
	cs := snap.GetCollection(collectionName)
	rv := Reader{c: cs}
	return &rv, nil
}

// Writer returns a KV writer
func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s: s}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
