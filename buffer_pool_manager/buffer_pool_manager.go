package buffer_pool_manager

import "errors"

func (bpm *BufferPoolManager) FetchPage(pageID uint32) (*Frame, error) {
	if index, exists := bpm.pageTable[pageID]; exists {
		bpm.frames[index].PinCount++
		return bpm.frames[index], nil
	}

	frameIndex, err := bpm.findFreeFrame()
	if err != nil {
		return nil, err
	}

	err = bpm.evictFrame(frameIndex)
	if err != nil {
		return nil, err
	}

	data, err := bpm.dm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	newFrame := NewFrame(pageID, data)

	bpm.frames[frameIndex] = newFrame
	bpm.pageTable[pageID] = frameIndex

	return newFrame, nil
}

func (bpm *BufferPoolManager) UnpinPage(pageID uint32, isDirty bool) error {
	if index, exists := bpm.pageTable[pageID]; exists {
		frame := bpm.frames[index]

		if frame.PinCount > 0 {
			frame.PinCount--
		}
		if isDirty {
			frame.Dirty = true
		}

		return nil
	}

	return errors.New("page not found")
}

func (bpm *BufferPoolManager) FlushPage(pageID uint32) error {
	if index, exists := bpm.pageTable[pageID]; exists {
		err := bpm.dm.WritePage(bpm.frames[index].PageID, bpm.frames[index].Data)
		if err != nil {
			return err
		}

		bpm.frames[index].Dirty = false
		return nil
	}

	return errors.New("page not found")
}

func (bpm *BufferPoolManager) NewPage() (newPageID uint32, newFrame *Frame, err error) {
	newPageID, err = bpm.dm.AllocatePage()
	if err != nil {
		return 0, nil, err
	}

	frameIndex, err := bpm.findFreeFrame()
	if err != nil {
		return 0, nil, err
	}

	err = bpm.evictFrame(frameIndex)
	if err != nil {
		return 0, nil, err
	}

	newFrame = NewFrame(newPageID, make([]byte, bpm.dm.PageSize))
	bpm.pageTable[newFrame.PageID] = frameIndex
	bpm.frames[frameIndex] = newFrame

	return newPageID, newFrame, nil
}
