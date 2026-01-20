package table_heap

import (
	"gobase/shared"
	"gobase/slotted_page"
)

func (ti *TableIterator) Next() (*RID, shared.Tuple, bool) {
	for {
		if ti.currentPageID == slotted_page.NULL_PAGE_ID {
			return nil, nil, false
		}

		frame, err := ti.th.bpm.FetchPage(uint32(ti.currentPageID))
		if err != nil {
			return nil, nil, false
		}

		sp := slotted_page.FromData(frame.Data)
		numSlots := sp.GetNumSlots()

		if ti.currentSlotID >= numSlots {
			nextPageID := sp.GetNextPageID()
			ti.th.bpm.UnpinPage(uint32(ti.currentPageID), false)
			ti.currentPageID = nextPageID
			ti.currentSlotID = 0
			continue
		}

		tuple, err := sp.GetTuple(ti.currentSlotID)
		if err != nil {
			ti.currentSlotID++
			ti.th.bpm.UnpinPage(uint32(ti.currentPageID), false)
			continue
		}

		rid := NewRID(ti.currentPageID, ti.currentSlotID)
		ti.currentSlotID++
		ti.th.bpm.UnpinPage(uint32(ti.currentPageID), false)

		return rid, tuple, true
	}
}
