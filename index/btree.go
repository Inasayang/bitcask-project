package index

import (
	"github.com/Inasayang/bitcask-project/data"
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (b *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(it)
	b.lock.Unlock()
	return true
}

func (b *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bit := b.tree.Get(it)
	if bit == nil {
		return nil
	}
	return bit.(*Item).pos
}

func (b *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	b.lock.Lock()
	oldItem := b.tree.Delete(it)
	b.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}
