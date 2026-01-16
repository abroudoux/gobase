package buffer_pool_manager

import "gobase/disk_manager"

type Frame struct {
	PageID   uint32
	Data     []byte
	Dirty    bool
	PinCount int
}

type BufferPoolManager struct {
	frames     []*Frame
	pageTable map[uint32]int
	dm         *disk_manager.DiskManager
	poolSize   int
}

func NewFrame(pageID uint32, data []byte) *Frame {
	return &Frame{
		PageID:   pageID,
		Data:     data,
		Dirty:    false,
		PinCount: 1,
	}
}

func NewBufferPoolManager(dm *disk_manager.DiskManager, poolSize int) *BufferPoolManager {
	frames := make([]*Frame, poolSize)
	pageTable := make(map[uint32]int)

	return &BufferPoolManager{
		frames:     frames,
		pageTable: pageTable,
		dm:         dm,
		poolSize:   poolSize,
	}
}
