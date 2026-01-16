package buffer_pool_manager

import "errors"

func (bpm *BufferPoolManager) findFreeFrame() (int, error) {
	for i, f := range bpm.frames {
		if f == nil {
			return i, nil
		}
	}

	for i, f := range bpm.frames {
		if f.PinCount == 0 {
			return i, nil
		}
	}

	return 0, errors.New("buffer pool full")
}

func (bpm *BufferPoolManager) evictFrame(frameIndex int) error {
	oldFrame := bpm.frames[frameIndex]
	if oldFrame == nil {
		return nil
	}

	if oldFrame.Dirty {
		err := bpm.dm.WritePage(oldFrame.PageID, oldFrame.Data)
		if err != nil {
			return err
		}
	}

	delete(bpm.pageTable, oldFrame.PageID)
	return nil
}
