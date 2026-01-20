package table_heap

func (r *RID) GetPageID() uint16 {
	return r.pageID
}

func (r *RID) GetSlotID() uint16 {
	return r.slotID
}

