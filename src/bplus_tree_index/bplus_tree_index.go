package bplus_tree_index

import (
	"gobase/src/shared"
	"slices"
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
			return in.Childs[i+1]
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

func (bpti *BPlusTreeIndex) Search(key uint16) (shared.RID, error) {
	node := findNodeRecursively(bpti.Root, key)
	if node == nil {
		return shared.RID{}, ErrKeyNotFound
	}

	leafNode := node.(*LeafNode)
	index := slices.Index(leafNode.Keys, key)
	if index == -1 {
		return shared.RID{}, ErrIndexDidntExists
	}

	return leafNode.RID[index], nil
}

func findNodeRecursively(node Node, key uint16) Node {
	switch n := node.(type) {
	case *InternalNode:
		nextNode := n.Search(key)
		return findNodeRecursively(nextNode, key)
	case *LeafNode:
		return n.Search(key)
	default:
		return nil
	}
}

func (bpti *BPlusTreeIndex) Insert(key uint16, RID shared.RID) {
	if bpti.Root == nil {
		bpti.Root = NewLeafNode([]uint16{key}, []shared.RID{RID})
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

func (bpti *BPlusTreeIndex) Delete(key uint16) error {
	var nodes []*InternalNode
	leafNode := bpti.findLeafNodeRecursively(key, &nodes, bpti.Root)
	if leafNode == nil {
		return ErrLeafNodeDidntExists
	}

	index := slices.Index(leafNode.Keys, key)
	if index == -1 {
		return ErrIndexDidntExists
	}

	leafNode.Keys = slices.Delete(leafNode.Keys, index, index+1)
	leafNode.RID = slices.Delete(leafNode.RID, index, index+1)

	if len(leafNode.Keys) < int(bpti.Order)/2 {
		if len(nodes) == 0 {
			return nil
		}

		parent := nodes[len(nodes)-1]
		leafNodeIndex := slices.Index[[]Node, Node](parent.Childs, leafNode)

		var leafNodeLeft, leafNodeRight *LeafNode

		if leafNodeIndex != 0 {
			siblingLeft := parent.Childs[leafNodeIndex-1]
			leafNodeLeft = siblingLeft.(*LeafNode)
		}
		if leafNodeIndex != len(parent.Childs)-1 {
			siblingRight := parent.Childs[leafNodeIndex+1]
			leafNodeRight = siblingRight.(*LeafNode)
		}

		if leafNodeLeft != nil && len(leafNodeLeft.Keys) > int(bpti.Order)/2 {
			borrowLeafFromLeft(leafNodeLeft, leafNode, parent, leafNodeIndex)
		} else if leafNodeRight != nil && len(leafNodeRight.Keys) > int(bpti.Order)/2 {
			borrowLeafFromRight(leafNodeRight, leafNode, parent, leafNodeIndex)
		} else {
			mergeLeafNodes(leafNodeLeft, leafNodeRight, leafNode, parent, leafNodeIndex)

			if len(nodes) == 1 && len(parent.Keys) == 0 {
				bpti.Root = parent.Childs[0]
				return nil
			}

			if len(parent.Keys) < int(bpti.Order)/2 {
				for i := len(nodes) - 2; i >= 0; i-- {
					currentNode := nodes[i]

					if len(currentNode.Keys) >= int(bpti.Order)/2 {
						break
					}

					if i == 0 && len(currentNode.Keys) == 0 {
						bpti.Root = currentNode.Childs[0]
						break
					}

					parent := nodes[i-1]
					currentNodeIndex := slices.Index[[]Node, Node](parent.Childs, currentNode)

					var internalNodeLeft, internalNodeRight *InternalNode

					if currentNodeIndex != 0 {
						siblingLeft := parent.Childs[currentNodeIndex-1]
						internalNodeLeft = siblingLeft.(*InternalNode)
					}
					if currentNodeIndex != len(parent.Childs)-1 {
						siblingRight := parent.Childs[currentNodeIndex+1]
						internalNodeRight = siblingRight.(*InternalNode)
					}

					if internalNodeLeft != nil && len(internalNodeLeft.Keys) > int(bpti.Order)/2 {
						borrowInternalFromLeft(currentNode, internalNodeLeft, parent, currentNodeIndex)
					} else if internalNodeRight != nil && len(internalNodeRight.Keys) > int(bpti.Order)/2 {
						borrowInternalFromRight(currentNode, internalNodeRight, parent, currentNodeIndex)
					} else {
						mergeInternalNodes(internalNodeLeft, internalNodeRight, currentNode, parent, currentNodeIndex)
					}
				}
			}
		}
	}

	return nil
}
