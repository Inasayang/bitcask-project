package kv

type IndexerType = int8

const (
	BTree IndexerType = iota + 1 //BTree 索引
	ART                          //ART Adpative Radix Tree 自适应基数树索引
)

type Options struct {
	Dir          string      // 数据库目录
	DataFileSize int64       // 数据文件大小
	SyncWrites   bool        // 每次写是否持久化
	IdxType      IndexerType //索引类型
}
