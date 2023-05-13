package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// CRC TYPE KEYSIZE VALUESIZE
//
//	4 +  1  +  5   +   5       = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	//TODO
	return nil, 0
}

type LogRecordHeader struct {
	crc        uint32 //crc校验值
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	//TODO
	return nil, 0
}
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	//TODO
	return 0
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
