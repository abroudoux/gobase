package bplus_tree_index

import "slices"

func findNodeIndex(keys []uint16, key uint16) (index int) {
	for i := range keys {
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

func borrowLeafFromLeft(leafNodeLeft, leafNode *LeafNode, parent *InternalNode, leafNodeIndex int) {
	lastKeyLeafNodeLeft := leafNodeLeft.Keys[len(leafNodeLeft.Keys)-1]
	lastRIDLeafNodeLeft := leafNodeLeft.RID[len(leafNodeLeft.RID)-1]

	leafNodeLeft.Keys = slices.Delete(leafNodeLeft.Keys, len(leafNodeLeft.Keys)-1, len(leafNodeLeft.Keys))
	leafNodeLeft.RID = slices.Delete(leafNodeLeft.RID, len(leafNodeLeft.RID)-1, len(leafNodeLeft.RID))

	leafNode.Keys = slices.Insert(leafNode.Keys, 0, lastKeyLeafNodeLeft)
	leafNode.RID = slices.Insert(leafNode.RID, 0, lastRIDLeafNodeLeft)

	parent.Keys[leafNodeIndex-1] = lastKeyLeafNodeLeft
}

func borrowLeafFromRight(leafNodeRight, leafNode *LeafNode, parent *InternalNode, leafNodeIndex int) {
	firstKeyLeafNodeRight := leafNodeRight.Keys[0]
	firstRIDLeafNodeRight := leafNodeRight.RID[0]

	leafNodeRight.Keys = slices.Delete(leafNodeRight.Keys, 0, 1)
	leafNodeRight.RID = slices.Delete(leafNodeRight.RID, 0, 1)

	insertIndex := len(leafNode.Keys)
	leafNode.Keys = slices.Insert(leafNode.Keys, insertIndex, firstKeyLeafNodeRight)
	leafNode.RID = slices.Insert(leafNode.RID, insertIndex, firstRIDLeafNodeRight)

	parent.Keys[leafNodeIndex] = leafNodeRight.Keys[0]
}

func mergeLeafNodes(leafNodeLeft, leafNodeRight, leafNode *LeafNode, parent *InternalNode, leafNodeIndex int) {
	if leafNodeLeft != nil {
		leafNodeLeft.Keys = append(leafNodeLeft.Keys, leafNode.Keys...)
		leafNodeLeft.RID = append(leafNodeLeft.RID, leafNode.RID...)

		leafNodeLeft.NextLeafNode = leafNode.NextLeafNode

		parent.Keys = slices.Delete(parent.Keys, leafNodeIndex-1, leafNodeIndex)
		parent.Childs = slices.Delete(parent.Childs, leafNodeIndex, leafNodeIndex+1)
	} else {
		leafNode.Keys = append(leafNode.Keys, leafNodeRight.Keys...)
		leafNode.RID = append(leafNode.RID, leafNodeRight.RID...)

		leafNode.NextLeafNode = leafNodeRight.NextLeafNode

		parent.Keys = slices.Delete(parent.Keys, leafNodeIndex, leafNodeIndex+1)
		parent.Childs = slices.Delete(parent.Childs, leafNodeIndex+1, leafNodeIndex+2)
	}
}

func borrowInternalFromLeft(currentNode, siblingLeft, parent *InternalNode, currentNodeIndex int) {
	lastKeyInternalNodeLeft := siblingLeft.Keys[len(siblingLeft.Keys)-1]
	lastChildInternalNodeLeft := siblingLeft.Childs[len(siblingLeft.Childs)-1]

	siblingLeft.Keys = slices.Delete(siblingLeft.Keys, len(siblingLeft.Keys)-1, len(siblingLeft.Keys))
	siblingLeft.Childs = slices.Delete(siblingLeft.Childs, len(siblingLeft.Childs)-1, len(siblingLeft.Childs))

	currentNode.Keys = slices.Insert(currentNode.Keys, 0, parent.Keys[currentNodeIndex-1])
	currentNode.Childs = slices.Insert(currentNode.Childs, 0, lastChildInternalNodeLeft)

	parent.Keys[currentNodeIndex-1] = lastKeyInternalNodeLeft
}

func borrowInternalFromRight(currentNode, siblingRight, parent *InternalNode, currentNodeIndex int) {
	firstKeyInternalNodeRight := siblingRight.Keys[0]
	firstChildsInternalNodeRight := siblingRight.Childs[0]

	siblingRight.Keys = slices.Delete(siblingRight.Keys, 0, 1)
	siblingRight.Childs = slices.Delete(siblingRight.Childs, 0, 1)

	insertIndex := len(currentNode.Keys)
	currentNode.Keys = slices.Insert(currentNode.Keys, insertIndex, parent.Keys[currentNodeIndex])
	currentNode.Childs = slices.Insert(currentNode.Childs, insertIndex, firstChildsInternalNodeRight)

	parent.Keys[currentNodeIndex] = firstKeyInternalNodeRight
}

func mergeInternalNodes(siblingLeft, siblingRight, currentNode, parent *InternalNode, currentNodeIndex int) {
	if siblingLeft != nil {
		siblingLeft.Keys = append(siblingLeft.Keys, parent.Keys[currentNodeIndex-1])
		siblingLeft.Keys = append(siblingLeft.Keys, currentNode.Keys...)
		siblingLeft.Childs = append(siblingLeft.Childs, currentNode.Childs...)

		parent.Keys = slices.Delete(parent.Keys, currentNodeIndex-1, currentNodeIndex)
		parent.Childs = slices.Delete(parent.Childs, currentNodeIndex, currentNodeIndex+1)
	} else {
		currentNode.Keys = append(currentNode.Keys, parent.Keys[currentNodeIndex])
		currentNode.Keys = append(currentNode.Keys, siblingRight.Keys...)
		currentNode.Childs = append(currentNode.Childs, siblingRight.Childs...)

		parent.Keys = slices.Delete(parent.Keys, currentNodeIndex, currentNodeIndex+1)
		parent.Childs = slices.Delete(parent.Childs, currentNodeIndex+1, currentNodeIndex+2)
	}
}
