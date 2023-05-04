package data

// LogRecordPos is the position of a log record in the log file
// 数据内存索引
type LogRecordPos struct {
	Fid    uint32 // file id
	Offset int64  // offset in the file
}
