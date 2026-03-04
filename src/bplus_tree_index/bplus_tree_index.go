package bplus_tree_index

import (
	"slices"

	"gobase/src/table_heap"
)

func (in *InternalNode) Search(key uint16) Node {
	if key < in.Keys[0] {
		return in.Childs[0]
	}

	for i := 0; i < (len(in.Keys) - 1); i++ {
		if key == in.Keys[i] {
			return in.Childs[i+1]
		}

		if key > in.Keys[i] && key < in.Keys[i+1] {
			return in.Childs[i]
		}
	}

	return in.Childs[len(in.Childs)-1]
}

func (ln *LeafNode) Search(key uint16) Node {
	if !slices.Contains(ln.Keys, key) {
		return nil
	}

	return ln
}

func (bpti *BPlusTreeIndex) Search(key uint16) Node {
	return findNodeRecursively(bpti.Root, key)
}

func findNodeRecursively(node Node, key uint16) Node {
	switch n := node.(type) {
	case *InternalNode:
		nextNode := n.Search(key)
		return findNodeRecursively(nextNode, key)
	case *LeafNode:
		return n
	default:
		return nil
	}
}

func (bpti *BPlusTreeIndex) Insert(key uint16, RID table_heap.RID) {
	if bpti.Root == nil {
		bpti.Root = NewLeafNode([]uint16{key}, []table_heap.RID{RID})
		return
	}

	var nodes []*InternalNode

	leafNode := bpti.findLeafNodeRecursively(key, &nodes, bpti.Root)
	index := findNodeIndex(leafNode.Keys, key)

	leafNode.Keys = slices.Insert(leafNode.Keys, index, key)
	leafNode.RID = slices.Insert(leafNode.RID, index, RID)

	if len(leafNode.Keys) >= int(bpti.Order) && len(leafNode.RID) >= int(bpti.Order) {
		middleKey, newLeafNode := splitLeafNode(leafNode)

		if len(nodes) == 0 {
			bpti.Root = NewInternalNode([]uint16{middleKey}, []Node{leafNode, newLeafNode})
		} else {
			var newInternalNode *InternalNode
			rootSplit := true

			for i := len(nodes) - 1; i >= 0; i-- {
				parent := nodes[i]
				index := findNodeIndex(parent.Keys, middleKey)

				parent.Keys = slices.Insert(parent.Keys, index, middleKey)
				parent.Childs = slices.Insert[[]Node, Node](parent.Childs, index+1, newLeafNode)

				if len(parent.Keys) < int(bpti.Order) {
					rootSplit = false
					break
				}

				middleKey, newInternalNode = splitInternalNode(parent)
			}

			if rootSplit {
				bpti.Root = NewInternalNode([]uint16{middleKey}, []Node{nodes[0], newInternalNode})
			}
		}
	}
}

func (bpti *BPlusTreeIndex) findLeafNodeRecursively(key uint16, nodes *[]*InternalNode, node Node) *LeafNode {
	switch n := node.(type) {
	case *InternalNode:
		*nodes = append(*nodes, n)
		return bpti.findLeafNodeRecursively(key, nodes, n.Search(key))
	case *LeafNode:
		return n
	default:
		return nil
	}
}

func findNodeIndex(keys []uint16, key uint16) (index int) {
	for i, _ := range keys {
		if key < keys[i] {
			return i
		}
	}

	return len(keys)
}

func splitLeafNode(ln *LeafNode) (uint16, *LeafNode) {
	middleIndex := len(ln.Keys) / 2
	middleKey := ln.Keys[middleIndex]

	newLeafNode := NewLeafNode(ln.Keys[middleIndex:], ln.RID[middleIndex:])

	ln.Keys = ln.Keys[:middleIndex]
	ln.RID = ln.RID[:middleIndex]
	ln.NextLeafNode = newLeafNode

	return middleKey, newLeafNode
}

func splitInternalNode(parent *InternalNode) (uint16, *InternalNode) {
	middleIndex := len(parent.Keys) / 2
	middleKey := parent.Keys[middleIndex]

	newInternalNode := NewInternalNode(parent.Keys[middleIndex+1:], parent.Childs[middleIndex+1:])

	parent.Keys = parent.Keys[:middleIndex]
	parent.Childs = parent.Childs[:middleIndex+1]

	return middleKey, newInternalNode
}
