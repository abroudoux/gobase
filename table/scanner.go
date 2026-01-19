package table

import "gobase/catalog"

func (ts *TableScanner) Next() ([]any, bool) {
	_, encodedData, ok := ts.iter.Next()
	if !ok {
		return nil, false
	}

	decodedData := catalog.DecodeTuple(ts.schema, encodedData)
	return decodedData, true
}
