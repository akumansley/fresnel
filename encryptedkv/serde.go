package encryptedkv

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"io"

	"golang.org/x/crypto/nacl/secretbox"
)

const nonceSize = 24

func (s *Store) loadFromFile() error {
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		s.seq = iter.Key().(int)
		encryptedBatch := iter.Value()

		// First 24 bytes of the encryptedBatch is the nonce
		copy(nonce[:], encryptedBatch[:nonceSize])
		batch, ok := secretbox.Open(nil, encryptedBatch[nonceSize:], nonce, s.key)
		reader := bytes.Reader(batch)
		decoder := gob.NewDecoder(reader)

		var kvList []Item
		err := decoder.Decode(&kvList)
		if err != nil {
			return err
		}

		// "Replay" the KVs into the treap
		for _, item := range kvList {
			if item.v == nil {
				s.treap = s.treap.Delete(item.k)
			} else {
				s.treap = s.treap.Upsert(item.k, item.v)
			}
		}
	}
	iter.Release()
	return iter.Error()
}

func writeBatchToFile(s *Store, items *[]Item) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(items)

	nonce := new([nonceSize]byte)
	_, err := io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		log.Fatalln("Could not read from random:", err)
	}
	out := make([]byte, nonceSize)
	copy(out, nonce[:])
	encryptedBatch := secretbox.Seal(out, buf.Bytes(), nonce, s.key)

	opts := leveldb.WriteOptions{} // what to do
	s.db.Put([]bytes(s.seq+1), encryptedBatch, &opts)
}
