package encryptedkv

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"log"
	mrand "math/rand"

	"golang.org/x/crypto/nacl/secretbox"
)

const nonceSize = 24

func (s *Store) loadFromFile() error {
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		keyReader := bytes.NewReader(iter.Key())
		seq, err := binary.ReadUvarint(keyReader)
		if err != nil {
			return err
		}
		s.seq = seq

		encryptedBatch := iter.Value()

		// First 24 bytes of the encryptedBatch is the nonce
		nonce := new([nonceSize]byte)
		copy(nonce[:], encryptedBatch[:nonceSize])
		batch, ok := secretbox.Open(nil, encryptedBatch[nonceSize:], nonce, &s.key)
		if !ok {
			return errors.New("Decryption failed")
		}
		reader := bytes.NewReader(batch)
		decoder := gob.NewDecoder(reader)

		var kvList []Item
		err = decoder.Decode(&kvList)
		if err != nil && err != io.EOF {
			return err
		}

		// "Replay" the KVs into the treap
		for _, item := range kvList {
			s.treap = s.treap.Upsert(&Item{K: item.K, V: item.V}, mrand.Int())
		}
	}
	iter.Release()
	return iter.Error()
}

func writeBatchToFile(s *Store, items []Item) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(items)

	nonce := new([nonceSize]byte)
	_, err = io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		log.Fatalln("Could not read from random:", err)
	}
	out := make([]byte, nonceSize)
	copy(out, nonce[:])
	encryptedBatch := secretbox.Seal(out, buf.Bytes(), nonce, &s.key)

	keyBuf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(keyBuf, s.seq+1)
	s.seq++
	s.db.Put(keyBuf, encryptedBatch, nil)
	return nil
}
