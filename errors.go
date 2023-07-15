package bitcask

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("key not found")
	ErrDataFileNotFound  = errors.New("data file not found")
	ErrDataDirCorrupted  = errors.New("database dir maybe corrupted")
)
