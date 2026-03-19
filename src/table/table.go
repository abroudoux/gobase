package table

import (
	"gobase/src/catalog"
	"gobase/src/shared"
)

func (t *Table) Insert(values ...any) (*shared.RID, error) {
	encodedData := catalog.EncodeTuple(t.Schema, values)

	rid, err := t.Heap.Insert(encodedData)
	if err != nil {
		return nil, err
	}

	if t.Index != nil {
		t.Index.Insert(uint16(values[t.IndexedColumn].(int)), *rid)
	}

	return rid, nil
}

func (t *Table) GetByRID(rid shared.RID) ([]any, error) {
	data, err := t.Heap.Get(rid)
	if err != nil {
		return nil, err
	}

	dataDecoded := catalog.DecodeTuple(t.Schema, data)

	return dataDecoded, nil
}

func (t *Table) Delete(rid shared.RID) error {
	if t.Index != nil {
		values, err := t.GetByRID(rid)
		if err != nil {
			return err
		}

		t.Index.Delete(uint16(values[t.IndexedColumn].(int)))
	}

	return t.Heap.Delete(rid)
}

func (t *Table) Scan() *TableScanner {
	return &TableScanner{
		schema: t.Schema,
		iter:   t.Heap.Scan(),
	}
}

func (t *Table) GetByKey(key uint16) ([]any, error) {
	if t.Index == nil {
		return nil, ErrIndexIsntSet
	}

	rid, err := t.Index.Search(key)
	if err != nil {
		return nil, err
	}

	tuple, err := t.GetByRID(rid)
	if err != nil {
		return nil, err
	}

	return tuple, nil
}
