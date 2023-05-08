package data

import "github.com/Inasayang/kv/fio"

const DataFileNameSuffix = ".data"

// DataFile数据文件
type DataFile struct {
	Fid       uint32
	Offset    int64
	IOManager fio.IOManager
}

func OpenDataFile(dir string, fid uint32) (*DataFile, error) {
	//TODO
	return nil, nil
}
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//TODO
	return nil, 0, nil
}
func (df *DataFile) Write(bytes []byte) error {
	//TODO
	return nil
}
func (df *DataFile) Sync() error {
	//TODO
	return nil
}
