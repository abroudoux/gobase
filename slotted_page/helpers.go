package slotted_page

import "encoding/binary"

func (sp *SlottedPage) getNumSlots() uint16 {
	return binary.LittleEndian.Uint16(sp.data[NUM_SLOTS_OFFSET:])
}

func (sp *SlottedPage) setNumSlots(n uint16) {
	binary.LittleEndian.PutUint16(sp.data[NUM_SLOTS_OFFSET:], n)
}

func (sp *SlottedPage) getFreeSpaceEnd() uint16 {
	return binary.LittleEndian.Uint16(sp.data[FREE_SPACE_END_OFFSET:])
}

func (sp *SlottedPage) setFreeSpaceEnd(offset uint16) {
	binary.LittleEndian.PutUint16(sp.data[FREE_SPACE_END_OFFSET:], offset)
}

func (sp *SlottedPage) getSlot(slotID uint16) (offset uint16, length uint16) {
	slotPos := HEADER_SIZE + slotID*SLOT_SIZE
	offset = binary.LittleEndian.Uint16(sp.data[slotPos:])
	length = binary.LittleEndian.Uint16(sp.data[slotPos+2:])
	return offset, length
}

func (sp *SlottedPage) setSlot(slotID uint16, offset uint16, length uint16) {
	slotPos := HEADER_SIZE + slotID*SLOT_SIZE
	binary.LittleEndian.PutUint16(sp.data[slotPos:], offset)
	binary.LittleEndian.PutUint16(sp.data[slotPos+2:], length)
}

func FromData(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

func (sp *SlottedPage) GetData() []byte {
	return sp.data
}
