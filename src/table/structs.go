package table

import (
	"gobase/src/bplus_tree_index"
	"gobase/src/catalog"
	"gobase/src/table_heap"
)

type Table struct {
	Name          string
	Schema        *catalog.Schema
	Heap          *table_heap.TableHeap
	Index         *bplus_tree_index.BPlusTreeIndex
	IndexedColumn int
}

type TableScanner struct {
	schema *catalog.Schema
	iter   *table_heap.TableIterator
}

func NewTable(name string, schema *catalog.Schema, heap *table_heap.TableHeap) *Table {
	return &Table{
		Name:          name,
		Schema:        schema,
		Heap:          heap,
		Index:         nil,
		IndexedColumn: 0,
	}
}

func NewTableWithIndex(name string, schema *catalog.Schema, heap *table_heap.TableHeap, index *bplus_tree_index.BPlusTreeIndex, indexedColumn int) *Table {
	return &Table{
		Name:          name,
		Schema:        schema,
		Heap:          heap,
		Index:         index,
		IndexedColumn: indexedColumn,
	}
}
