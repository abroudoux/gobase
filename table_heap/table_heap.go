package table_heap

import (
	"gobase/shared"
	"gobase/slotted_page"
)

func (th *TableHeap) Insert(tuple shared.Tuple) (*RID, error) {
	lastFrame, err := th.bpm.FetchPage(uint32(th.lastPageID))
	if err != nil {
		return nil, err
	}

	sp := slotted_page.FromData(lastFrame.Data)

	spaceNeeded := len(tuple) + int(slotted_page.SLOT_SIZE)
	if spaceNeeded <= int(sp.GetFreeSpace()) {
		slotID, err := sp.InsertTuple(tuple)
		if err != nil {
			th.bpm.UnpinPage(uint32(th.lastPageID), false)
			return nil, err
		}

		th.bpm.UnpinPage(uint32(th.lastPageID), true)
		return NewRID(th.lastPageID, slotID), nil
	}

	th.bpm.UnpinPage(uint32(th.lastPageID), false)

	newPageID, newFrame, err := th.bpm.NewPage()
	if err != nil {
		return nil, err
	}

	slotted_page.InitSlottedPage(newFrame.Data)
	newSp := slotted_page.FromData(newFrame.Data)

	slotID, err := newSp.InsertTuple(tuple)
	if err != nil {
		th.bpm.UnpinPage(newPageID, false)
		return nil, err
	}

	th.lastPageID = uint16(newPageID)

	th.bpm.UnpinPage(newPageID, true)
	return NewRID(uint16(newPageID), slotID), nil
}

func (th *TableHeap) Get(rid RID) (shared.Tuple, error) {
	frame, err := th.bpm.FetchPage(uint32(rid.pageID))
	if err != nil {
		return nil, err
	}

	sp := slotted_page.FromData(frame.Data)

	tuple, err := sp.GetTuple(rid.slotID)
	if err != nil {
		th.bpm.UnpinPage(uint32(rid.pageID), false)
		return nil, err
	}

	th.bpm.UnpinPage(uint32(rid.pageID), false)
	return tuple, nil
}

func (th *TableHeap) Delete(rid RID) error {
	frame, err := th.bpm.FetchPage(uint32(rid.pageID))
	if err != nil {
		return err
	}

	sp := slotted_page.FromData(frame.Data)
	err = sp.DeleteTuple(rid.slotID)
	if err != nil {
		th.bpm.UnpinPage(uint32(rid.pageID), false)
		return err
	}

	th.bpm.UnpinPage(uint32(rid.pageID), true)
	return nil
}

func (th *TableHeap) Scan() *TableIterator {
	return &TableIterator{
		tableHeap:     th,
		currentPageID: th.firstPageID,
		currentSlotID: 0,
	}
}
