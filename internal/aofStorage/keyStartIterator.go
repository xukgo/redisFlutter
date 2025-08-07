package aofStorage

import (
	"bytes"
	"encoding/binary"
	"io"
)

type KeyStartIterator struct {
	commitKeys    [][]byte
	buffer        *bytes.Buffer
	startKeyIndex uint64

	//formatKeyBuffer []byte
}

func NewKeyStartIterator(initCacheSize int64) *KeyStartIterator {
	cache := make([]byte, 0, initCacheSize)
	c := &KeyStartIterator{
		commitKeys:    make([][]byte, 0, 1024),
		startKeyIndex: 0,
		buffer:        bytes.NewBuffer(cache),
		//formatKeyBuffer: make([]byte, 8),
	}
	return c
}

func (c *KeyStartIterator) Reset() {
	c.commitKeys = c.commitKeys[:0]
	c.buffer.Reset()
	//c.formatKeyBuffer = c.formatKeyBuffer[:0]
	c.startKeyIndex = 0
}

func (c *KeyStartIterator) SetStartKey(idx uint64) {
	c.startKeyIndex = idx
}

func (c *KeyStartIterator) GetCurrentKeyIndex() uint64 {
	return c.startKeyIndex
}
func (c *KeyStartIterator) GetWriter() io.Writer {
	return c.buffer
}

func (c *KeyStartIterator) GetBufferCap() int {
	return c.buffer.Cap()
}

func (c *KeyStartIterator) NextKey() []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, c.startKeyIndex)
	c.startKeyIndex++
	return buffer
	//return strconv.AppendInt(c.formatKeyBuffer, idx, 10)
}

func (c *KeyStartIterator) CommitKey(key []byte) {
	c.commitKeys = append(c.commitKeys, key)
}

func (c *KeyStartIterator) GetCommitKeys() [][]byte {
	return c.commitKeys
}
