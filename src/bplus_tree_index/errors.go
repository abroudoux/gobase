package bplus_tree_index

import "errors"

var (
	ErrKeyNotFound         = errors.New("key is not found")
	ErrIndexDidntExists    = errors.New("index didn't exists")
	ErrLeafNodeDidntExists = errors.New("leafnode didn't exists")
)
