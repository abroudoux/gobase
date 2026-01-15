package buffer_pool_manager

import "errors"

func (bpm *BufferPoolManager) findFreeFrame() (int, error) {
	for i, p := range bpm.pages {
		if p == nil {
			return i, nil
		}
	}

	for i, p := range bpm.pages {
		if p.PinCount == 0 {
			return i, nil
		}
	}

	return 0, errors.New("buffer pool full")
}

func (bpm *BufferPoolManager) evictPage(frameIndex int) error {
	oldPage := bpm.pages[frameIndex]
	if oldPage == nil {
		return nil
	}

	if oldPage.Dirty {
		err := bpm.dm.WritePage(oldPage.ID, oldPage.Data)
		if err != nil {
			return err
		}
	}

	delete(bpm.pageTable, oldPage.ID)
	return nil
}
