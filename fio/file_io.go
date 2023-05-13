package fio

import "os"

const (
	DataFilePerm = 0644
)

// FileIO is a file IO manager
type FileIO struct {
	fd *os.File
}

func (f *FileIO) Read(bytes []byte, i int64) (int, error) {
	return f.fd.ReadAt(bytes, i)
}

func (f *FileIO) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}
func (f *FileIO) Size() (int64, error) {
	stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func NewFileIOManager(fn string) (*FileIO, error) {
	fd, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd}, nil
}
