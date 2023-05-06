package kv

import (
	"github.com/Inasayang/kv/data"
	"github.com/Inasayang/kv/index"
	"sync"
)

// DB  存储引擎实例
type DB struct {
	opt        Options
	lock       *sync.RWMutex
	activeFile *data.DataFile            //当前活跃数据文件，用于写入
	olderFiles map[uint32]*data.DataFile //旧数据文件，只读
	idx        index.Indexer             //内存索引
}

// Put 写入kv数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}
	if ok := db.idx.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := data.EncodeLogRecord(logRecord)   //编码数据
	if db.activeFile.Offset+size > db.opt.DataFileSize { //写入的数据达到活跃文件阙值，则关闭，写入新文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.olderFiles[db.activeFile.Fid] = db.activeFile
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	offset := db.activeFile.Offset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	if db.opt.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.Fid,
		Offset: offset,
	}
	return pos, nil

}

// setActiveDataFile 设置当前活跃文件
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32
	if db.activeFile != nil {
		initialFileID = db.activeFile.Fid + 1
	}
	dataFile, err := data.OpenDataFile(db.opt.Dir, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := d.idx.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	var dataFile *data.DataFile
	if d.activeFile.Fid == logRecordPos.Fid {
		dataFile = d.activeFile
	} else {
		dataFile = d.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}
