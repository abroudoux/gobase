package bplus_tree_index

import "gobase/src/shared"

type Node interface {
	Search(key uint16) Node
}

type InternalNode struct {
	Keys   []uint16
	Childs []Node
}

type LeafNode struct {
	Keys         []uint16
	RID          []shared.RID
	NextLeafNode *LeafNode
}

type BPlusTreeIndex struct {
	Root  Node
	Order uint8
}

func NewInternalNode(keys []uint16, childs []Node) *InternalNode {
	return &InternalNode{
		Keys:   keys,
		Childs: childs,
	}
}

func NewLeafNode(keys []uint16, RIDs []shared.RID) *LeafNode {
	return &LeafNode{
		Keys:         keys,
		RID:          RIDs,
		NextLeafNode: nil,
	}
}
