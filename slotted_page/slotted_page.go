package slotted_page

import (
	"encoding/binary"
	"errors"

	"gobase/shared"
)

func (sp *SlottedPage) GetFreeSpace() uint16 {
	return sp.getFreeSpaceEnd() - (HEADER_SIZE + sp.getNumSlots()*SLOT_SIZE)
}

func (sp *SlottedPage) InsertTuple(tuple shared.Tuple) (slotID uint16, err error) {
	spaceRequired := len(tuple) + int(SLOT_SIZE)
	if spaceRequired > int(sp.GetFreeSpace()) {
		return 0, errors.New("not enough space")
	}

	newTupleOffset := sp.getFreeSpaceEnd() - uint16(len(tuple))
	copy(sp.data[newTupleOffset:], tuple)

	slotID = sp.getNumSlots()

	sp.setSlot(slotID, newTupleOffset, uint16(len(tuple)))
	sp.setNumSlots(sp.getNumSlots() + 1)
	sp.setFreeSpaceEnd(newTupleOffset)

	return slotID, nil
}

func (sp *SlottedPage) GetTuple(slotID uint16) (shared.Tuple, error) {
	if slotID >= sp.getNumSlots() {
		return nil, errors.New("slot didn't exists")
	}

	offset, length := sp.getSlot(slotID)
	if length == 0 {
		return nil, errors.New("tuple has been deleted")
	}

	tuple := make(shared.Tuple, length)
	copy(tuple, sp.data[offset:offset+length])

	return tuple, nil
}

func (sp *SlottedPage) DeleteTuple(slotID uint16) error {
	if slotID >= sp.getNumSlots() {
		return errors.New("slot didn't exists")
	}

	offset, length := sp.getSlot(slotID)
	if length == 0 {
		return errors.New("slot has already been deleted")
	}

	sp.setSlot(slotID, offset, 0)

	return nil
}

func FromData(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

func (sp *SlottedPage) GetData() []byte {
	return sp.data
}

func InitSlottedPage(data []byte) {
	binary.LittleEndian.PutUint16(data[NUM_SLOTS_OFFSET:], 0)
	binary.LittleEndian.PutUint16(data[FREE_SPACE_END_OFFSET:], uint16(shared.PAGE_SIZE))
}
