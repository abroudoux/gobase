package disk_manager

import "errors"

var (
	ErrPageDoesNotExist    = errors.New("page does not exist")
	ErrIncompleteRead      = errors.New("incomplete read")
	ErrInvalidPageDataSize = errors.New("data size does not match page size")
	ErrWriteFailed         = errors.New("write failed")
	ErrAllocatePageFailed  = errors.New("failed to allocate page")
	ErrOpenFileFailed      = errors.New("failed to open file")
	ErrStatFileFailed      = errors.New("failed to stat file")
)