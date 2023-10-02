package lsmtree

import (
	"github.com/Khighness/gokit/rbtree"
)

// @Author KHighness
// @Update 2023-09-29

// memTable is memory cache of SSTable.
// All changed that are flushed to the WAL, but not flushed to
// the sorted files, are sorted in memory for faster lookups.
type memTable struct {
	data *rbtree.Tree
	// b is the size of al the keys and values inserted into
	b int
}

// newMemTable creates a new instance of the MemTable.
func newMemTable() *memTable {
	return &memTable{
		data: rbtree.New(),
		b:    0,
	}
}

// put puts the key and value into the table.
func (mt *memTable) put(key, value []byte) error {
	prev, exists := mt.data.Put(key, value)
	if exists {
		mt.b += -len(prev) + len(value)
	} else {
		mt.b += len(key) + len(value)
	}

	return nil
}

// get returns thr value according to the key.
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.data.Get(key)
}

// delete marks the key as deleted in the table, but does not remove it.
func (mt *memTable) delete(key []byte) error {
	value, exists := mt.data.Put(key, nil)
	if !exists {
		mt.b += len(key)
	} else {
		mt.b -= len(value)
	}

	return nil
}

// bytes returns the size of all keys and values inserted into thd memTable in bytes.
func (mt *memTable) bytes() int {
	return mt.b
}

// clear clears all the data and resets the size.
func (mt *memTable) clear() {
	mt.data = rbtree.New()
	mt.b = 0
}

// iterator returns iterator for MemTable. It also iterates over
// deleted keys, but the value for them is nil.
func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{mt.data.Iterator()}
}

// memTableIterator is iterator of MemTable.
type memTableIterator struct {
	it *rbtree.Iterator
}

// hasNext returns true if there is next element.
func (it *memTableIterator) hasNext() bool {
	return it.it.HasNext()
}

// next returns thr current key and value and advances thr iterator position.
func (it *memTableIterator) next() ([]byte, []byte) {
	return it.it.Next()
}
