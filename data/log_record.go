package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// CRC TYPE KEYSIZE VALUESIZE
//
//	4 +  1  +  5   +   5       = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	index := 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	size := index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

type LogRecordHeader struct {
	crc        uint32 //crc校验值
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	index := 5
	//Varint编码使用7个比特位来存储整数的每个字节，其中最高位用于指示是否还有后续字节。如果最高位为0，表示当前字节是编码的最后一个字节；如果最高位为1，表示后续字节还需要进行编码。
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)

}
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
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
