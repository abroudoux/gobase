package slotted_page

import (
	"encoding/binary"

	"gobase/shared"
)

type SlottedPage struct {
	data []byte
}

type Slot struct {
	Offset uint16
	Length uint16
}

func NewSlottedPage() *SlottedPage {
	data := make([]byte, shared.PAGE_SIZE)

	binary.LittleEndian.PutUint16(data[NUM_SLOTS_OFFSET:], 0)
	binary.LittleEndian.PutUint16(data[FREE_SPACE_END_OFFSET:], uint16(shared.PAGE_SIZE))

	return &SlottedPage{data: data}
}
