package slotted_page

import (
	"gobase/shared"
)

func (sp *SlottedPage) GetFreeSpace() uint16 {
	return sp.getFreeSpaceEnd() - (HEADER_SIZE + sp.GetNumSlots()*SLOT_SIZE)
}

func (sp *SlottedPage) InsertTuple(tuple shared.Tuple) (slotID uint16, err error) {
	spaceRequired := len(tuple) + int(SLOT_SIZE)
	if spaceRequired > int(sp.GetFreeSpace()) {
		return 0, ErrNotEnoughSpace
	}

	newTupleOffset := sp.getFreeSpaceEnd() - uint16(len(tuple))
	copy(sp.data[newTupleOffset:], tuple)

	slotID = sp.GetNumSlots()

	sp.setSlot(slotID, newTupleOffset, uint16(len(tuple)))
	sp.setNumSlots(sp.GetNumSlots() + 1)
	sp.setFreeSpaceEnd(newTupleOffset)

	return slotID, nil
}

func (sp *SlottedPage) GetTuple(slotID uint16) (shared.Tuple, error) {
	if slotID >= sp.GetNumSlots() {
		return nil, ErrorSlotDidntExists
	}

	offset, length := sp.getSlot(slotID)
	if length == 0 {
		return nil, ErrTupleHasBeenDeleted
	}

	tuple := make(shared.Tuple, length)
	copy(tuple, sp.data[offset:offset+length])

	return tuple, nil
}

func (sp *SlottedPage) DeleteTuple(slotID uint16) error {
	if slotID >= sp.GetNumSlots() {
		return ErrorSlotDidntExists
	}

	offset, length := sp.getSlot(slotID)
	if length == 0 {
		return ErrTupleHasBeenDeleted
	}

	sp.setSlot(slotID, offset, 0)

	return nil
}
