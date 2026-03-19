package shared

type Tuple []byte

type RID struct {
	PageID uint16
	SlotID uint16
}

func NewTuple(data string) Tuple {
	return Tuple(data)
}

func NewRID(pageID, slotID uint16) *RID {
	return &RID{
		PageID: pageID,
		SlotID: slotID,
	}
}
