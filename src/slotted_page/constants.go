package slotted_page

const (
	HEADER_SIZE uint16 = 8
	SLOT_SIZE   uint16 = 4

	NEXT_PAGE_ID_OFFSET uint16 = 4
	PREV_PAGE_ID_OFFSET uint16 = 6
	NULL_PAGE_ID = uint16(0xFFFF)

	NUM_SLOTS_OFFSET      uint16 = 0
	FREE_SPACE_END_OFFSET uint16 = 2
)
