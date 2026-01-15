package buffer_pool_manager

import "gobase/disk_manager"

type Page struct {
	ID       uint32
	Data     []byte
	Dirty    bool
	PinCount int
}

func NewPage(id uint32, data []byte) *Page {
	return &Page{
		ID:       id,
		Data:     data,
		Dirty:    false,
		PinCount: 1,
	}
}

type BufferPoolManager struct {
	pages     []*Page
	pageTable map[uint32]int
	dm        *disk_manager.DiskManager
	poolSize  int
}

func NewBufferPoolManager(dm *disk_manager.DiskManager, poolSize int) *BufferPoolManager {
	pages := make([]*Page, poolSize)
	pageTable := make(map[uint32]int)

	return &BufferPoolManager{
		pages:     pages,
		pageTable: pageTable,
		dm:        dm,
		poolSize:  poolSize,
	}
}
