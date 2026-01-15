package disk_manager

func calculateOffset(pageID uint32, pageSize uint32) int64 {
	return int64(pageID) * int64(pageSize)
}
