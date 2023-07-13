package data

import (
	"errors"
	"fmt"
	"github.com/Inasayang/bitcask-project/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var (
	ErrInvaliedCRC = errors.New("invalid crc, log record maybe corrupted")
)

// DataFile数据文件
type DataFile struct {
	Fid       uint32
	Offset    int64
	IOManager fio.IOManager
}

func OpenDataFile(dir string, fid uint32) (*DataFile, error) {
	fileName := filepath.Join(dir, fmt.Sprintf("%09d", fid)+DataFileNameSuffix)
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		Fid:       fid,
		Offset:    0,
		IOManager: ioManager,
	}, nil
}
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	//如果最大header长度超过了文件的长度，只读到文件末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize
	logRecord := &LogRecord{
		Type: header.recordType,
	}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvaliedCRC
	}
	return logRecord, recordSize, nil
}
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.Offset += int64(n)
	return nil
}
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}
