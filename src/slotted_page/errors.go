package slotted_page

import "errors"

var (
	ErrNotEnoughSpace = errors.New("not enought space")
	ErrorSlotDidntExists = errors.New("slot didn't exists")
	ErrTupleHasBeenDeleted = errors.New("tuple has been deleted")
)
