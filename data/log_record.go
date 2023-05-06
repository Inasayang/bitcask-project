package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	//TODO
	return nil, 0
}

// LogRecord is a log record 写入到数据文件的记录
// 数据文件中的数据是追加写入，WAL
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos is the position of a log record in the log file
// 数据内存索引
type LogRecordPos struct {
	Fid    uint32 // file id
	Offset int64  // offset in the file
}
