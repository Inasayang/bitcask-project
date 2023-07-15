package index

import (
	"sync"

	"github.com/Inasayang/bitcask-project/data"
	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (b *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	t := &Item{key: key, pos: pos}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(t)
	b.lock.Unlock()
	return true
}

func (b *BTree) Get(key []byte) *data.LogRecordPos {
	k := &Item{key: key}
	t := b.tree.Get(k)
	if t == nil {
		return nil
	}
	return t.(*Item).pos
}

func (b *BTree) Delete(key []byte) bool {
	k := &Item{key: key}
	b.lock.Lock()
	t := b.tree.Delete(k)
	b.lock.Unlock()
	return t!=nil
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}
