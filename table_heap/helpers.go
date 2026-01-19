package table_heap

func (r *RID) GetPageID() uint16 {
	return r.pageID
}

func (r *RID) GetSlotID() uint16 {
	return r.slotID
}

func initSlottedPageHeader(data []byte) {
	// numSlots = 0
	data[0] = 0
	data[1] = 0
	// freeSpaceEnd = 4096 (little endian)
	data[2] = 0x00
	data[3] = 0x10
}
