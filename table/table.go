package table

import (
	"gobase/catalog"
	"gobase/table_heap"
)

func (t *Table) Insert(values ...any) (*table_heap.RID, error) {
	encodedData := catalog.EncodeTuple(t.Schema, values)

	rid, err := t.Heap.Insert(encodedData)
	if err != nil {
		return nil, err
	}

	return rid, nil
}

func (t *Table) GetByRID(rid table_heap.RID) ([]any, error) {
	data, err := t.Heap.Get(rid)
	if err != nil {
		return nil, err
	}

	dataDecoded := catalog.DecodeTuple(t.Schema, data)

	return dataDecoded, nil
}

func (t *Table) Delete(rid table_heap.RID) error {
	return t.Heap.Delete(rid)
}

func (t *Table) Scan() *TableScanner {
	return &TableScanner{
		schema: t.Schema,
		iter:   t.Heap.Scan(),
	}
}
