package disk_manager

func (dm *DiskManager) ReadPage(pageID uint32) (pageData []byte, err error) {
	if pageID >= dm.NumPages {
		return nil, ErrPageDoesNotExist
	}

	offset := calculateOffset(pageID, dm.PageSize)

	data := make([]byte, dm.PageSize)
	n, err := dm.File.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	if uint32(n) != dm.PageSize {
		return nil, ErrIncompleteRead
	}

	return data, nil
}

func (dm *DiskManager) WritePage(pageID uint32, data []byte) error {
	if pageID >= dm.NumPages {
		return ErrPageDoesNotExist
	}

	if uint32(len(data)) != dm.PageSize {
		return ErrInvalidPageDataSize
	}

	offset := calculateOffset(pageID, dm.PageSize)

	_, err := dm.File.WriteAt(data, offset)
	if err != nil {
		return ErrWriteFailed
	}

	return dm.File.Sync()
}

func (dm *DiskManager) AllocatePage() (newPageID uint32, err error) {
	newPageID = dm.NumPages

	offset := calculateOffset(dm.NumPages, dm.PageSize)

	emptyPage := make([]byte, dm.PageSize)
	_, err = dm.File.WriteAt(emptyPage, offset)
	if err != nil {
		return 0, ErrAllocatePageFailed
	}

	dm.File.Sync()
	dm.NumPages += 1

	return newPageID, nil
}

func (dm *DiskManager) Close() error {
	dm.File.Sync()
	return dm.File.Close()
}
