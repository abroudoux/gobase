package buffer_pool_manager

import (
	"testing"

	"gobase/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindFreeFrame_EmptyPool(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	index, err := bpm.findFreeFrame()
	require.NoError(t, err)
	assert.Equal(t, 0, index)
}

func TestFindFreeFrame_PartiallyFilled(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	bpm.frames[0] = NewFrame(0, make([]byte, shared.PAGE_SIZE))
	bpm.frames[1] = NewFrame(1, make([]byte, shared.PAGE_SIZE))

	index, err := bpm.findFreeFrame()
	require.NoError(t, err)
	assert.Equal(t, 2, index)
}

func TestFindFreeFrame_FullPoolWithUnpinnedFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	for i := 0; i < 3; i++ {
		bpm.frames[i] = NewFrame(uint32(i), make([]byte, shared.PAGE_SIZE))
		bpm.frames[i].PinCount = 1
	}

	bpm.frames[1].PinCount = 0

	index, err := bpm.findFreeFrame()
	require.NoError(t, err)
	assert.Equal(t, 1, index)
}

func TestFindFreeFrame_FullPoolAllPinned(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	for i := 0; i < 3; i++ {
		bpm.frames[i] = NewFrame(uint32(i), make([]byte, shared.PAGE_SIZE))
		bpm.frames[i].PinCount = 1
	}

	_, err := bpm.findFreeFrame()
	require.ErrorIs(t, err, ErrBufferPoolFull)
}

func TestEvictFrame_NilFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	err := bpm.evictFrame(0)
	require.NoError(t, err)
}

func TestEvictFrame_CleanFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 3)
	defer cleanup()

	pageID := uint32(0)
	bpm.frames[0] = NewFrame(pageID, make([]byte, shared.PAGE_SIZE))
	bpm.frames[0].Dirty = false
	bpm.pageTable[pageID] = 0

	err := bpm.evictFrame(0)
	require.NoError(t, err)
	assert.NotContains(t, bpm.pageTable, pageID)
}

func TestEvictFrame_DirtyFrame(t *testing.T) {
	dm, cleanup := newTestDiskManager(t)
	defer cleanup()

	_, err := dm.AllocatePage()
	require.NoError(t, err)

	bpm := NewBufferPoolManager(dm, 3)

	pageID := uint32(0)
	data := make([]byte, shared.PAGE_SIZE)
	copy(data, []byte("dirty data"))

	bpm.frames[0] = NewFrame(pageID, data)
	bpm.frames[0].Dirty = true
	bpm.pageTable[pageID] = 0

	err = bpm.evictFrame(0)
	require.NoError(t, err)
	assert.NotContains(t, bpm.pageTable, pageID)

	readData, err := dm.ReadPage(pageID)
	require.NoError(t, err)
	assert.Equal(t, data, readData)
}
