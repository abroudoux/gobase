package table

import (
	"gobase/catalog"
	"gobase/table_heap"
)

type Table struct {
	Name   string
	Schema *catalog.Schema
	Heap   *table_heap.TableHeap
}

type TableScanner struct {
	schema *catalog.Schema
	iter   *table_heap.TableIterator
}

func NewTable(name string, schema *catalog.Schema, heap *table_heap.TableHeap) *Table {
	return &Table{
		Name:   name,
		Schema: schema,
		Heap:   heap,
	}
}
