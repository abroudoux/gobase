package buffer_pool_manager

import "errors"

var (
	ErrBufferPoolFull    = errors.New("buffer pool is full")
	ErrPageNotFound = errors.New("page not found")
)