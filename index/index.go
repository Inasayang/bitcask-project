package index

import (
	"bytes"

	"github.com/Inasayang/bitcask-project/data"
	"github.com/google/btree"
)

// Memory

// Indexer 通用索引接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	Btree IndexType = iota + 1 // Btree 索引
	ART                        // ART 自适应基数树索引
)
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//TODO
		return nil
	default:
		panic("unsupported index type")
	}
}
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
