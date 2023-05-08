package kv

import (
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Inasayang/kv/data"
	"github.com/Inasayang/kv/index"
)

// DB  存储引擎实例
type DB struct {
	opt        Options
	lock       *sync.RWMutex
	fileIDs    []int                     //文件ID，在加载索引时使用，不在其他地方跟新和使用
	activeFile *data.DataFile            //当前活跃数据文件，用于写入
	olderFiles map[uint32]*data.DataFile //旧数据文件，只读
	idx        index.Indexer             //内存索引
}

func checkOptions(opt Options) error {
	if opt.Dir == "" {
		return errors.New("database directory is empty")
	}
	if opt.DataFileSize <= 0 {
		return errors.New("data file size <= 0 ")
	}
	return nil
}
func Open(opt Options) (*DB, error) {
	if err := checkOptions(opt); err != nil {
		return nil, err
	}
	if _, err := os.Stat(opt.Dir); os.IsNotExist(err) {
		if err := os.MkdirAll(opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	db := &DB{
		opt:        opt,
		lock:       &sync.RWMutex{},
		fileIDs:    []int{},
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		idx:        index.NewIndexer(opt.IdxType),
	}
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.opt.Dir)
	if err != nil {
		return err
	}
	var fileIDs []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIDs = append(fileIDs, fileID)
		}
	}
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs
	for i, fid := range fileIDs {
		dataFile, err := data.OpenDataFile(db.opt.Dir, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIDs)-1 { //最后一个，id是最大，说明是当前活跃文件
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并跟新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，数据库为空
	if len(db.fileIDs) == 0 {
		return nil
	}
	for i, fid := range db.fileIDs {
		var fileID = uint32(fid)
		var dataFile *data.DataFile
		if fileID == db.activeFile.Fid {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileID]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileID,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDeleted {
				db.idx.Delete(logRecord.Key)
			} else {
				db.idx.Put(logRecord.Key, logRecordPos)
			}
			offset += size
		}
		//如果是当前活跃文件，跟新这个文件的offset
		if i == len(db.fileIDs)-1 {
			db.activeFile.Offset = offset
		}
	}
	return nil
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}
