package disk_manager

import (
	"os"

	"gobase/shared"
)

type DiskManager struct {
	File     *os.File
	PageSize uint32
	NumPages uint32
}

func NewDiskManager(filePath string) (*DiskManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, ErrOpenFileFailed
	}

	stats, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, ErrStatFileFailed
	}

	numPages := stats.Size() / int64(shared.PAGE_SIZE)

	newDiskManager := &DiskManager{
		File:     file,
		PageSize: shared.PAGE_SIZE,
		NumPages: uint32(numPages),
	}

	return newDiskManager, nil
}
