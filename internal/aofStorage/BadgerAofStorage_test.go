package aofStorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"strconv"
	"testing"
)

func test_repeatSetBytes(buffer []byte, v byte) []byte {
	for i := 0; i < len(buffer); i++ {
		buffer[i] = v
	}
	return buffer
}

func Test_BadgerAofStorage01(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBuffer := make([]byte, 0, 32)
	for i := 0; i < 10000; i++ {
		binary.BigEndian.PutUint64(keyBuffer, uint64(i))

		valBuffer = valBuffer[:0]
		valBuffer = strconv.AppendInt(valBuffer, int64(i), 10)
		err = target.Append(keyBuffer, valBuffer)
		if err != nil {
			t.Error(err)
		}
	}

	iterator := NewKeyStartIterator(1024 * 64)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys := iterator.GetCommitKeys()
	ckIndex := 0
	assert.Equal(t, iterator.buffer.Len(), 38890)
	assert.Equal(t, len(commitKeys), 10000)
	buffer := bytes.Buffer{}
	for i := 0; i < 10000; i++ {
		decodedValue := binary.BigEndian.Uint64(commitKeys[ckIndex])
		assert.True(t, decodedValue == uint64(i))
		ckIndex++

		buffer.WriteString(fmt.Sprintf("%d", i))
	}
	assert.Equal(t, buffer.String(), iterator.buffer.String())
	err = target.DeleteArray(iterator.GetCommitKeys())
	if err != nil {
		t.Error(err)
	}
	iterator.Reset()
	buffer.Reset()
}

func Test_BadgerAofStorage02(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBuffer := make([]byte, 0, 32)

	binary.BigEndian.PutUint64(keyBuffer, uint64(0))
	valBuffer = valBuffer[:0]
	valBuffer = strconv.AppendInt(valBuffer, int64(0), 10)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(1))
	valBuffer = make([]byte, 1024)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(2))
	valBuffer = make([]byte, 1024*2)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(3))
	valBuffer = make([]byte, 1024*3)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(4))
	valBuffer = make([]byte, 1024*2)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(5))
	valBuffer = make([]byte, 200)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	iterator := NewKeyStartIterator(1024)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys := iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 1)
	assert.Equal(t, len(commitKeys), 1)

	err = target.DeleteArray(iterator.GetCommitKeys())
	if err != nil {
		t.Error(err)
	}
	startKeyIndex := iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 1)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 1024)
	assert.Equal(t, len(iterator.GetCommitKeys()), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 2)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 1024*2)
	assert.Equal(t, len(commitKeys), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 3)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 1024*3)
	assert.Equal(t, len(commitKeys), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 4)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 2048+200)
	assert.Equal(t, len(commitKeys), 2)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 6)
	iterator.Reset()
}

func Test_BadgerAofStorage03(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBuffer := make([]byte, 0, 32)

	binary.BigEndian.PutUint64(keyBuffer, uint64(0))
	valBuffer = make([]byte, 300)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(1))
	valBuffer = make([]byte, 800)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(2))
	valBuffer = make([]byte, 1024*2)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	iterator := NewKeyStartIterator(1024)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys := iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 300)
	assert.Equal(t, len(commitKeys), 1)

	err = target.DeleteArray(iterator.GetCommitKeys())
	if err != nil {
		t.Error(err)
	}
	startKeyIndex := iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 1)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 800)
	assert.Equal(t, len(iterator.GetCommitKeys()), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 2)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 1024*2)
	assert.Equal(t, len(commitKeys), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 3)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 0)
	assert.Equal(t, len(commitKeys), 0)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 3)
	iterator.Reset()
}

func Test_BadgerAofStorage04(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBuffer := make([]byte, 0, 32)

	binary.BigEndian.PutUint64(keyBuffer, uint64(0))
	valBuffer = make([]byte, 600)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(1))
	valBuffer = make([]byte, 2048)
	valBuffer = test_repeatSetBytes(valBuffer, 1)
	err = target.Append(keyBuffer, valBuffer)
	if err != nil {
		t.Error(err)
	}

	iterator := NewKeyStartIterator(512)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys := iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 600)
	assert.Equal(t, len(commitKeys), 1)

	err = target.DeleteArray(iterator.GetCommitKeys())
	if err != nil {
		t.Error(err)
	}
	startKeyIndex := iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 1)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 2048)
	assert.Equal(t, len(iterator.GetCommitKeys()), 1)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 2)
	iterator.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	assert.Equal(t, iterator.buffer.Len(), 0)
	assert.Equal(t, len(commitKeys), 0)
	startKeyIndex = iterator.GetCurrentKeyIndex()
	assert.True(t, startKeyIndex == 2)
	iterator.Reset()
}

func Test_BadgerAofStorage_check_value_reuse_single_buffer(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBufferCount := 32
	valBuffer0 := make([]byte, valBufferCount)
	valBuffer1 := make([]byte, valBufferCount)

	binary.BigEndian.PutUint64(keyBuffer, uint64(0))
	valBuffer0 = test_repeatSetBytes(valBuffer0, 1)
	err = target.Append(keyBuffer, valBuffer0)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(1))
	valBuffer1 = test_repeatSetBytes(valBuffer1, 2)
	err = target.Append(keyBuffer, valBuffer1)
	if err != nil {
		t.Error(err)
	}

	binary.BigEndian.PutUint64(keyBuffer, uint64(0))
	actualValBuffer, err := target.Read(keyBuffer)
	if err != nil {
		t.FailNow()
	}
	assert.True(t, bytes.Equal(actualValBuffer, valBuffer0))

	binary.BigEndian.PutUint64(keyBuffer, uint64(1))
	actualValBuffer, err = target.Read(keyBuffer)
	if err != nil {
		t.FailNow()
	}
	assert.True(t, bytes.Equal(actualValBuffer, valBuffer1))
}

func Test_BadgerAofStorage09(t *testing.T) {
	uid := uuid.NewV1().String()
	dataPath := filepath.Join("/tmp", uid)
	target, err := NewBadgerAofStorage(dataPath)
	defer target.Destroy()

	if err != nil {
		t.Error(err)
	}

	keyBuffer := make([]byte, 8)
	valBuffer := make([]byte, 0, 32)
	for i := 0; i < 10000; i++ {
		binary.BigEndian.PutUint64(keyBuffer, uint64(i))

		valBuffer = valBuffer[:0]
		valBuffer = strconv.AppendInt(valBuffer, int64(i), 10)
		err = target.Append(keyBuffer, valBuffer)
		if err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < 10000; i++ {
		valBuffer = valBuffer[:0]
		valBuffer = strconv.AppendInt(valBuffer, int64(i), 10)
		binary.BigEndian.PutUint64(keyBuffer, uint64(i))
		vb, err := target.Read(keyBuffer)
		if err != nil {
			t.Error(err)
		}

		assert.True(t, bytes.Equal(valBuffer, vb))
	}

	iterator := NewKeyStartIterator(1024)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys := iterator.GetCommitKeys()
	ckIndex := 0
	assert.Equal(t, iterator.buffer.Len(), 1024)
	assert.Equal(t, len(commitKeys), 378)
	buffer := bytes.Buffer{}
	for i := 0; i < 378; i++ {
		decodedValue := binary.BigEndian.Uint64(commitKeys[ckIndex])
		assert.True(t, decodedValue == uint64(i))
		ckIndex++

		buffer.WriteString(fmt.Sprintf("%d", i))
	}
	assert.Equal(t, buffer.String(), iterator.buffer.String())
	err = target.DeleteArray(iterator.GetCommitKeys())
	if err != nil {
		t.Error(err)
	}
	startKeyIndex := iterator.GetCurrentKeyIndex()
	iterator.Reset()
	buffer.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	ckIndex = 0
	assert.Equal(t, iterator.buffer.Len(), 1023)
	assert.Equal(t, len(iterator.GetCommitKeys()), 341)
	for i := 378; i < 719; i++ {
		decodedValue := binary.BigEndian.Uint64(commitKeys[ckIndex])
		assert.True(t, decodedValue == uint64(i))
		ckIndex++

		buffer.WriteString(fmt.Sprintf("%d", i))
	}
	assert.Equal(t, buffer.String(), iterator.buffer.String())
	startKeyIndex = iterator.GetCurrentKeyIndex()
	iterator.Reset()
	buffer.Reset()

	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	ckIndex = 0
	assert.Equal(t, iterator.buffer.Len(), 1023)
	assert.Equal(t, len(iterator.GetCommitKeys()), 326)
	for i := 719; i < 1045; i++ {
		decodedValue := binary.BigEndian.Uint64(commitKeys[ckIndex])
		assert.True(t, decodedValue == uint64(i))
		ckIndex++

		buffer.WriteString(fmt.Sprintf("%d", i))
	}
	assert.Equal(t, buffer.String(), iterator.buffer.String())
	startKeyIndex = iterator.GetCurrentKeyIndex()
	iterator.Reset()
	buffer.Reset()

	iterator = NewKeyStartIterator(1024 * 100)
	iterator.SetStartKey(startKeyIndex)
	err = target.ReadFunc(iterator)
	if err != nil {
		t.Error(err)
	}
	commitKeys = iterator.GetCommitKeys()
	ckIndex = 0
	assert.Equal(t, iterator.buffer.Len(), 35820)
	assert.Equal(t, len(iterator.GetCommitKeys()), 8955)
	for i := 1045; i < 10000; i++ {
		decodedValue := binary.BigEndian.Uint64(commitKeys[ckIndex])
		assert.True(t, decodedValue == uint64(i))
		ckIndex++

		buffer.WriteString(fmt.Sprintf("%d", i))
	}
	assert.Equal(t, buffer.String(), iterator.buffer.String())
	iterator.Reset()
	buffer.Reset()
}
