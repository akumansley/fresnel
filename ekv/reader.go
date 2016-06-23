//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package ekv

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/steveyen/gkvlite"
)

// Reader implements Reader
type Reader struct {
	c *gkvlite.Collection
}

// Get implements Reader
func (r *Reader) Get(k []byte) (v []byte, err error) {
	var rv []byte
	val, err := r.c.Get(k)
	if val != nil {
		rv = make([]byte, len(val))
		copy(rv, val)
		return rv, nil
	}
	return nil, err

}

// MultiGet implements Reader
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

// PrefixIterator implements Reader
func (r *Reader) PrefixIterator(k []byte) store.KVIterator {
	rv := Iterator{
		c:      r.c,
		prefix: k,
	}
	rv.restart(&gkvlite.Item{Key: k})
	return &rv
}

// RangeIterator implements Reader
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := Iterator{
		c:     r.c,
		start: start,
		end:   end,
	}
	rv.restart(&gkvlite.Item{Key: start})
	return &rv
}

// Close implements Reader
func (r *Reader) Close() error {
	r.c = nil
	return nil
}
