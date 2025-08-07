package aofStorage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func keyStartIterator_equal_init_status(t *testing.T, target *KeyStartIterator, maxCap int) {
	assert.Equal(t, target.GetBufferCap(), maxCap)
	assert.Equal(t, len(target.GetCommitKeys()), 0)
	assert.True(t, target.GetWriter() != nil)
	assert.True(t, target.buffer.Cap() == int(maxCap))

}
func Test_KeyStartIterator01(t *testing.T) {
	target := NewKeyStartIterator(32)
	keyStartIterator_equal_init_status(t, target, 32)
	target.SetStartKey(123)
	assert.Equal(t, target.NextKey(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 123})
	assert.Equal(t, target.NextKey(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 124})
	assert.True(t, target.buffer.Cap() == int(32))

	data := make([]byte, target.buffer.Cap())
	for i := 0; i < len(data); i++ {
		data[i] = byte(i)
	}
	target.GetWriter().Write(data)
	target.CommitKey([]byte("001"))
	assert.Equal(t, len(target.GetCommitKeys()), 1)
	target.CommitKey([]byte("002"))
	assert.Equal(t, len(target.GetCommitKeys()), 2)

	target.Reset()
	keyStartIterator_equal_init_status(t, target, 32)

	target.GetWriter().Write(data)
	target.GetWriter().Write(data)
	assert.True(t, target.buffer.Cap() == int(64))
}
