package table_heap

import (
	"gobase/src/buffer_pool_manager"
	"gobase/src/slotted_page"
)

type TableHeap struct {
	bpm         *buffer_pool_manager.BufferPoolManager
	firstPageID uint16
	lastPageID  uint16
}

type TableIterator struct {
	th            *TableHeap
	currentPageID uint16
	currentSlotID uint16
}

func NewTableHeap(bpm *buffer_pool_manager.BufferPoolManager) (*TableHeap, error) {
	pageID, frame, err := bpm.NewPage()
	if err != nil {
		return nil, err
	}

	slotted_page.InitSlottedPage(frame.Data)

	bpm.UnpinPage(pageID, true)

	return &TableHeap{
		bpm:         bpm,
		firstPageID: uint16(pageID),
		lastPageID:  uint16(pageID),
	}, nil
}
