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
	"bytes"
	"sync"

	"github.com/steveyen/gkvlite"
)

// Iterator implements blevesearch.Iterator
type Iterator struct {
	c *gkvlite.Collection

	m        sync.Mutex
	cancelCh chan struct{}
	nextCh   chan *gkvlite.Item
	curr     *gkvlite.Item
	currOk   bool

	prefix []byte
	start  []byte
	end    []byte
}

// Seek implements iterator
func (w *Iterator) Seek(k []byte) {
	if w.start != nil && bytes.Compare(k, w.start) < 0 {
		k = w.start
	}
	if w.prefix != nil && !bytes.HasPrefix(k, w.prefix) {
		if bytes.Compare(k, w.prefix) < 0 {
			k = w.prefix
		} else {
			var end []byte
			for i := len(w.prefix) - 1; i >= 0; i-- {
				c := w.prefix[i]
				if c < 0xff {
					end = make([]byte, i+1)
					copy(end, w.prefix)
					end[i] = c + 1
					break
				}
			}
			k = end
		}
	}
	w.restart(&gkvlite.Item{Key: k})
}

func (w *Iterator) restart(start *gkvlite.Item) *Iterator {
	cancelCh := make(chan struct{})
	nextCh := make(chan *gkvlite.Item, 1)

	w.m.Lock()
	if w.cancelCh != nil {
		close(w.cancelCh)
	}
	w.cancelCh = cancelCh
	w.nextCh = nextCh
	w.curr = nil
	w.currOk = false
	w.m.Unlock()

	go func() {
		if start != nil {
			w.c.VisitItemsAscend(start.Key, true, func(itm *gkvlite.Item) bool {
				select {
				case <-cancelCh:
					return false
				case nextCh <- itm:
					return true
				}
			})
		}
		close(nextCh)
	}()

	w.Next()

	return w
}

// Next implements Iterator
func (w *Iterator) Next() {
	w.m.Lock()
	nextCh := w.nextCh
	w.m.Unlock()
	w.curr, w.currOk = <-nextCh
}

// Current implements Iterator
func (w *Iterator) Current() ([]byte, []byte, bool) {
	w.m.Lock()
	defer w.m.Unlock()
	if !w.currOk || w.curr == nil {
		return nil, nil, false
	}
	if w.prefix != nil && !bytes.HasPrefix(w.curr.Key, w.prefix) {
		return nil, nil, false
	} else if w.end != nil && bytes.Compare(w.curr.Key, w.end) >= 0 {
		return nil, nil, false
	}
	return w.curr.Key, w.curr.Val, w.currOk
}

// Key implements Iterator
func (w *Iterator) Key() []byte {
	k, _, ok := w.Current()
	if !ok {
		return nil
	}
	return k
}

// Value implements Iterator
func (w *Iterator) Value() []byte {
	_, v, ok := w.Current()
	if !ok {
		return nil
	}
	return v
}

// Valid implements Iterator
func (w *Iterator) Valid() bool {
	_, _, ok := w.Current()
	return ok
}

// Close implements Iterator
func (w *Iterator) Close() error {
	w.m.Lock()
	if w.cancelCh != nil {
		close(w.cancelCh)
	}
	w.cancelCh = nil
	w.nextCh = nil
	w.curr = nil
	w.currOk = false
	w.m.Unlock()

	return nil
}
