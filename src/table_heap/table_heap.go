package table_heap

import (
	"gobase/src/shared"
	"gobase/src/slotted_page"
)

func (th *TableHeap) Insert(tuple shared.Tuple) (*shared.RID, error) {
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
		return shared.NewRID(th.lastPageID, slotID), nil
	}

	newPageID, newFrame, err := th.bpm.NewPage()
	if err != nil {
		th.bpm.UnpinPage(uint32(th.lastPageID), false)
		return nil, err
	}

	slotted_page.InitSlottedPage(newFrame.Data)
	newSp := slotted_page.FromData(newFrame.Data)

	sp.SetNextPageID(uint16(newPageID))
	newSp.SetPrevPageID(th.lastPageID)

	slotID, err := newSp.InsertTuple(tuple)
	if err != nil {
		th.bpm.UnpinPage(uint32(th.lastPageID), false)
		th.bpm.UnpinPage(newPageID, false)
		return nil, err
	}

	oldLastPageID := th.lastPageID
	th.lastPageID = uint16(newPageID)

	th.bpm.UnpinPage(uint32(oldLastPageID), true)
	th.bpm.UnpinPage(newPageID, true)
	return shared.NewRID(uint16(newPageID), slotID), nil
}

func (th *TableHeap) Get(rid shared.RID) (shared.Tuple, error) {
	frame, err := th.bpm.FetchPage(uint32(rid.PageID))
	if err != nil {
		return nil, err
	}

	sp := slotted_page.FromData(frame.Data)

	tuple, err := sp.GetTuple(rid.SlotID)
	if err != nil {
		th.bpm.UnpinPage(uint32(rid.PageID), false)
		return nil, err
	}

	th.bpm.UnpinPage(uint32(rid.PageID), false)
	return tuple, nil
}

func (th *TableHeap) Delete(rid shared.RID) error {
	frame, err := th.bpm.FetchPage(uint32(rid.PageID))
	if err != nil {
		return err
	}

	sp := slotted_page.FromData(frame.Data)
	err = sp.DeleteTuple(rid.SlotID)
	if err != nil {
		th.bpm.UnpinPage(uint32(rid.PageID), false)
		return err
	}

	th.bpm.UnpinPage(uint32(rid.PageID), true)
	return nil
}

func (th *TableHeap) Scan() *TableIterator {
	return &TableIterator{
		th:            th,
		currentPageID: th.firstPageID,
		currentSlotID: 0,
	}
}
