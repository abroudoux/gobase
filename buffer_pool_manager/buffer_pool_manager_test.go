package buffer_pool_manager

import (
	"testing"

	"gobase/disk_manager"
	"gobase/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchPage_PageAlreadyInBuffer(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 0,
  	}
	bpm.pageTable[0] = 0

	frame, err := bpm.FetchPage(0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), frame.PageID)
	assert.Equal(t, 1, frame.PinCount)
	assert.Equal(t, false, frame.Dirty)
}

func TestFetchPage_LoadFromDisk(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	bpm.dm.AllocatePage()

	frame, err := bpm.FetchPage(0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0),frame.PageID)
	assert.Equal(t, 0, bpm.pageTable[0])
}

func TestFetchPage_EvictCleanFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)
	_, err = bpm.dm.AllocatePage()
	require.NoError(t, err)

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 0,
	}
	bpm.pageTable[0] = 0

	frame, err := bpm.FetchPage(1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), frame.PageID)
	assert.Equal(t, 1, frame.PinCount)
	assert.NotContains(t, bpm.pageTable, uint32(0))
	assert.Equal(t, 0, bpm.pageTable[1])
}

func TestFetchPage_EvictDirtyFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)
	_, err = bpm.dm.AllocatePage()
	require.NoError(t, err)

	dirtyData := make([]byte, shared.PAGE_SIZE)
	copy(dirtyData, []byte("dirty data"))

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     dirtyData,
		Dirty:    true,
		PinCount: 0,
	}
	bpm.pageTable[0] = 0

	frame, err := bpm.FetchPage(1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), frame.PageID)
	assert.NotContains(t, bpm.pageTable, uint32(0))

	dataOnDisk, err := bpm.dm.ReadPage(0)
	require.NoError(t, err)
	assert.Equal(t, dirtyData, dataOnDisk)
}

func TestFetchPage_BufferPoolFull(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)
	_, err = bpm.dm.AllocatePage()
    require.NoError(t, err)

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	_, err = bpm.FetchPage(1)
	require.ErrorIs(t, err, ErrBufferPoolFull)
}

func TestFetchPage_PageDoesNotExist(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.FetchPage(0)
	require.ErrorIs(t, err, disk_manager.ErrPageDoesNotExist)
}

func TestUnpinPage(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	err = bpm.UnpinPage(0, false)
	require.NoError(t, err)
	assert.Equal(t, 0, bpm.frames[0].PinCount)
}

func TestUnpinPage_IsDirty(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	err = bpm.UnpinPage(0, true)
	require.NoError(t, err)
	assert.Equal(t, 0, bpm.frames[0].PinCount)
	assert.Equal(t, true, bpm.frames[0].Dirty)
}

func TestUnpinPage_PageNotFound(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	err := bpm.UnpinPage(0, false)
	require.ErrorIs(t, err, ErrPageNotFound)
}

func TestFlushPage(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)

	data := make([]byte, shared.PAGE_SIZE)
	copy(data, []byte("flushed data"))

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     data,
		Dirty:    true,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	err = bpm.FlushPage(0)
	require.NoError(t, err)
	assert.Equal(t, false, bpm.frames[0].Dirty)

	dataOnDisk, err := bpm.dm.ReadPage(0)
	require.NoError(t, err)
	assert.Equal(t, data, dataOnDisk)
}

func TestFlushPage_CleanPage(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)

	data := make([]byte, shared.PAGE_SIZE)
	copy(data, []byte("clean data"))

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     data,
		Dirty:    false,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	err = bpm.FlushPage(0)
	require.NoError(t, err)
	assert.Equal(t, false, bpm.frames[0].Dirty)

	dataOnDisk, err := bpm.dm.ReadPage(0)
	require.NoError(t, err)
	assert.Equal(t, data, dataOnDisk)
}

func TestFlushPage_PageNotFound(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	err := bpm.FlushPage(0)
	require.ErrorIs(t, err, ErrPageNotFound)
}

func TestNewPage(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	newPageID, newFrame, err := bpm.NewPage()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), newPageID)
	assert.Equal(t, newPageID, newFrame.PageID)
	assert.Equal(t, 1, newFrame.PinCount)
	assert.Equal(t, 0, bpm.pageTable[newPageID])
	assert.Equal(t, newFrame, bpm.frames[0])
}

func TestNewPage_BufferPoolFull(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     make([]byte, shared.PAGE_SIZE),
		Dirty:    false,
		PinCount: 1,
	}
	bpm.pageTable[0] = 0

	_, _, err := bpm.NewPage()
	require.ErrorIs(t, err, ErrBufferPoolFull)
}

func TestNewPage_EvictDirtyFrame(t *testing.T) {
	bpm, cleanup := newTestBufferPoolManager(t, 1)
	defer cleanup()

	_, err := bpm.dm.AllocatePage()
	require.NoError(t, err)

	dirtyData := make([]byte, shared.PAGE_SIZE)
	copy(dirtyData, []byte("dirty data"))

	bpm.frames[0] = &Frame{
		PageID:   0,
		Data:     dirtyData,
		Dirty:    true,
		PinCount: 0,
	}
	bpm.pageTable[0] = 0

	newPageID, newFrame, err := bpm.NewPage()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), newPageID)
	assert.Equal(t, newPageID, newFrame.PageID)
	assert.NotContains(t, bpm.pageTable, uint32(0))

	dataOnDisk, err := bpm.dm.ReadPage(0)
	require.NoError(t, err)
	assert.Equal(t, dirtyData, dataOnDisk)
}
