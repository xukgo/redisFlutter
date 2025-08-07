package aofStorage

import "io"

type DbWriteIterator interface {
	GetBufferCap() int
	GetWriter() io.Writer
	SetStartKey(int64)
	//AppendMaxCap(int64)
	NextKey() []byte
	CommitKey([]byte)
	GetCommitKeys() [][]byte
	Reset()
}

type Storage interface {
	Append(key []byte, value []byte) error
	DeleteArray(keys [][]byte) error
	ReadFunc(iterator DbWriteIterator) error
	Destroy()
}
