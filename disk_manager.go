package main

import (
	"errors"
	"os"
)

type DiskManager interface {
	ReadPage(pageID uint32) ([]byte, error)
	WritePage(pageID uint32, data []byte) error
	AllocatePage() (uint32, error)
	Close() error
}

type diskManager struct {
	file     *os.File
	pageSize uint32
	numPages uint32
}

const PAGE_SIZE uint32 = 4096

func NewDiskManager(filePath string) (*diskManager, error) {
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

	newDiskManager := &diskManager{
		file:     file,
		pageSize: PAGE_SIZE,
		numPages: uint32(numPages),
	}

	return newDiskManager, nil
}

func (dm *diskManager) ReadPage(pageID uint32) ([]byte, error) {
	if pageID >= dm.numPages {
		return nil, errors.New("page does not exist")
	}

	offset := int64(pageID) * int64(dm.pageSize)

	data := make([]byte, dm.pageSize)
	n, err := dm.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	if uint32(n) != dm.pageSize {
		return nil, errors.New("incomplete read")
	}

	return data, nil
}

func (dm *diskManager) WritePage(pageID uint32, data []byte) error {
	if pageID >= dm.numPages {
		return errors.New("page does not exist")
	}

	if uint32(len(data)) != dm.pageSize {
		return errors.New("data size does not match page size")
	}

	offset := int64(pageID) * int64(dm.pageSize)

	_, err := dm.file.WriteAt(data, offset)
	if err != nil {
		return errors.New("error writing data")
	}

	return dm.file.Sync()
}

func (dm *diskManager) AllocatePage() (uint32, error) {
	newPageId := dm.numPages

	offset := int64(dm.numPages) * int64(dm.pageSize)

	emptyPage := make([]byte, dm.pageSize)
	_, err := dm.file.WriteAt(emptyPage, offset)
	if err != nil {
		return 0, errors.New("error creating new page")
	}

	dm.file.Sync()
	dm.numPages += 1

	return newPageId, nil
}

func (dm *diskManager) Close() error {
	dm.file.Sync()
	return dm.file.Close()
}
