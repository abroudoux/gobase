package buffer_pool_manager

import "errors"

func (bpm *BufferPoolManager) FetchPage(pageID uint32) (*Page, error) {
	if index, exists := bpm.pageTable[pageID]; exists {
		bpm.pages[index].PinCount++
		return bpm.pages[index], nil
	}

	frameIndex, err := bpm.findFreeFrame()
	if err != nil {
		return nil, err
	}

	err = bpm.evictPage(frameIndex)
	if err != nil {
		return nil, err
	}

	data, err := bpm.dm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	newPage := NewPage(pageID, data)

	bpm.pages[frameIndex] = newPage
	bpm.pageTable[pageID] = frameIndex

	return newPage, nil
}

func (bpm *BufferPoolManager) UnpinPage(pageID uint32, isDirty bool) error {
	if index, exists := bpm.pageTable[pageID]; exists {
		pageFound := bpm.pages[index]

		if pageFound.PinCount > 0 {
			pageFound.PinCount--
		}
		if isDirty {
			pageFound.Dirty = true
		}

		return nil
	}

	return errors.New("page not found")
}

func (bpm *BufferPoolManager) FlushPage(pageID uint32) error {
	if index, exists := bpm.pageTable[pageID]; exists {
		err := bpm.dm.WritePage(bpm.pages[index].ID, bpm.pages[index].Data)
		if err != nil {
			return err
		}

		bpm.pages[index].Dirty = false
		return nil
	}

	return errors.New("page not found")
}

func (bpm *BufferPoolManager) NewPage() (uint32, *Page, error) {
	newPageId, err := bpm.dm.AllocatePage()
	if err != nil {
		return 0, nil, err
	}

	frameIndex, err := bpm.findFreeFrame()
	if err != nil {
		return 0, nil, err
	}

	err = bpm.evictPage(frameIndex)
	if err != nil {
		return 0, nil, err
	}

	newPage := NewPage(newPageId, make([]byte, bpm.dm.PageSize))
	bpm.pageTable[newPage.ID] = frameIndex
	bpm.pages[frameIndex] = newPage

	return newPage.ID, newPage, nil
}
