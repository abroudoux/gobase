package table_heap

import "gobase/buffer_pool_manager"

type RID struct {
	pageID uint16
	slotID uint16
}

type TableHeap struct {
	bpm         *buffer_pool_manager.BufferPoolManager
	firstPageID uint16
	lastPageID  uint16
}

type TableIterator struct {
	tableHeap     *TableHeap
	currentPageID uint16
	currentSlotID uint16
}

func NewRID(pageID, slotID uint16) *RID {
	return &RID{
		pageID: pageID,
		slotID: slotID,
	}
}
