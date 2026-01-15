package disk_manager

import (
	"errors"
)

func (dm *DiskManager) ReadPage(pageID uint32) ([]byte, error) {
	if pageID >= dm.NumPages {
		return nil, errors.New("page does not exist")
	}

	offset := calculateOffset(pageID, dm.PageSize)

	data := make([]byte, dm.PageSize)
	n, err := dm.File.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	if uint32(n) != dm.PageSize {
		return nil, errors.New("incomplete read")
	}

	return data, nil
}

func (dm *DiskManager) WritePage(pageID uint32, data []byte) error {
	if pageID >= dm.NumPages {
		return errors.New("page does not exist")
	}

	if uint32(len(data)) != dm.PageSize {
		return errors.New("data size does not match page size")
	}

	offset := calculateOffset(pageID, dm.PageSize)

	_, err := dm.File.WriteAt(data, offset)
	if err != nil {
		return errors.New("error writing data")
	}

	return dm.File.Sync()
}

func (dm *DiskManager) AllocatePage() (uint32, error) {
	newPageId := dm.NumPages

	offset := calculateOffset(dm.NumPages, dm.PageSize)

	emptyPage := make([]byte, dm.PageSize)
	_, err := dm.File.WriteAt(emptyPage, offset)
	if err != nil {
		return 0, errors.New("error creating new page")
	}

	dm.File.Sync()
	dm.NumPages += 1

	return newPageId, nil
}

func (dm *DiskManager) Close() error {
	dm.File.Sync()
	return dm.File.Close()
}
