package buffer_pool_manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFrame(t *testing.T) {
	data := []byte("ABCDEFGH")
	f := NewFrame(uint32(1), data)

	assert.Equal(t, uint32(1), f.PageID)
	assert.Equal(t, data, f.Data)
	assert.Equal(t, false, f.Dirty)
	assert.Equal(t, 1, f.PinCount)
}

func TestNewBufferPoolManager(t *testing.T) {
	dm, cleanup := newTestDiskManager(t)
	defer cleanup()

	poolSize := 10
	bpm := NewBufferPoolManager(dm, poolSize)

	assert.NotNil(t, bpm)
	assert.Equal(t, poolSize, bpm.poolSize)
	assert.Equal(t, poolSize, len(bpm.frames))
	assert.Empty(t, bpm.pageTable)
	assert.Equal(t, dm, bpm.dm)
}
