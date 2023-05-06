package kv

type Options struct {
	Dir          string // 数据库目录
	DataFileSize int64  // 数据文件大小
	SyncWrites   bool   // 每次写是否持久化
}
