package disk_manager

import (
	"errors"
	"os"
)

type DiskManager struct {
	File     *os.File
	PageSize uint32
	NumPages uint32
}

func NewDiskManager(filePath string) (*DiskManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("error during file reading")
	}

	fileSize, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, errors.New("error during file stats reading")
	}

	numPages := fileSize.Size() / int64(PAGE_SIZE)

	newDiskManager := &DiskManager{
		File:     file,
		PageSize: PAGE_SIZE,
		NumPages: uint32(numPages),
	}

	return newDiskManager, nil
}
