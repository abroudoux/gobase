package slotted_page

import (
	"fmt"
	"testing"

	"gobase/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertTuple(t *testing.T) {
	sp := NewSlottedPage()

	newTuple := shared.NewTuple("data_test")

	slotID, err := sp.InsertTuple(newTuple)
	require.NoError(t, err)

	offset, length := sp.getSlot(slotID)
	tuple := make(shared.Tuple, length)
	copy(tuple, sp.data[offset:offset+length])

	assert.Equal(t, tuple, newTuple)
	assert.Equal(t, len(tuple), len(newTuple))
}

func TestInsertTuple_ErrNotEnoughSpace(t *testing.T) {
	sp := NewSlottedPage()

	bigTuple := make(shared.Tuple, shared.PAGE_SIZE+1)

	_, err := sp.InsertTuple(bigTuple)
	require.ErrorIs(t, err, ErrNotEnoughSpace)
}

func TestInsertTuple_MultipleInserts(t *testing.T) {
	sp := NewSlottedPage()

	for i := 0; i < 3; i++ {
		tuple := shared.NewTuple(fmt.Sprintf("data_%d", i))
		slotID, err := sp.InsertTuple(tuple)
		require.NoError(t, err)
		assert.Equal(t, uint16(i), slotID)
	}
	assert.Equal(t, uint16(3), sp.GetNumSlots())
  }

func TestGetTuple(t *testing.T) {
	sp := NewSlottedPage()

	tupleData := []byte("data_test")
	offset := uint16(shared.PAGE_SIZE) - uint16(len(tupleData))
	copy(sp.data[offset:], tupleData)
	sp.setSlot(0, offset, uint16(len(tupleData)))
	sp.setNumSlots(1)
	sp.setFreeSpaceEnd(offset)

	tuple, err := sp.GetTuple(0)
	require.NoError(t, err)
	assert.Equal(t, shared.Tuple(tupleData), tuple)
}

func TestGetTuple_ErrSlotDidntExists(t *testing.T) {
	sp := NewSlottedPage()

	_, err := sp.GetTuple(0)
	require.ErrorIs(t, err, ErrorSlotDidntExists)
}

func TestGetTuple_ErrTupleHasBeenDelete(t *testing.T) {
	sp := NewSlottedPage()

	tupleData := []byte("data_test")
	offset := uint16(shared.PAGE_SIZE) - uint16(len(tupleData))
	copy(sp.data[offset:], tupleData)
	sp.setSlot(0, offset, 0)
	sp.setNumSlots(1)
	sp.setFreeSpaceEnd(offset)

	_, err := sp.GetTuple(0)
	require.ErrorIs(t, err, ErrTupleHasBeenDeleted)
}

func TestDeleteTuple(t *testing.T) {
	sp := NewSlottedPage()

	tupleData := []byte("data_test")
	offset := uint16(shared.PAGE_SIZE) - uint16(len(tupleData))
	copy(sp.data[offset:], tupleData)
	sp.setSlot(0, offset, uint16(len(tupleData)))
	sp.setNumSlots(1)
	sp.setFreeSpaceEnd(offset)

	err := sp.DeleteTuple(0)
	require.NoError(t, err)

	_, length := sp.getSlot(0)
	assert.Equal(t, uint16(0), length)
}

func TestDeleteTuple_ErrSlotDidntExists(t *testing.T) {
	sp := NewSlottedPage()

	err := sp.DeleteTuple(0)
	require.ErrorIs(t, err, ErrorSlotDidntExists)
}

func TestDeleteTuple_ErrTupleHasBeenDeleted(t *testing.T) {
	sp := NewSlottedPage()

	tupleData := []byte("data_test")
	offset := uint16(shared.PAGE_SIZE) - uint16(len(tupleData))
	copy(sp.data[offset:], tupleData)
	sp.setSlot(0, offset, 0)
	sp.setNumSlots(1)
	sp.setFreeSpaceEnd(offset)

	err := sp.DeleteTuple(0)
	require.ErrorIs(t, err, ErrTupleHasBeenDeleted)
}
