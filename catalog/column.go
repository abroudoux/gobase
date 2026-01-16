package catalog

import "errors"

func (s *Schema) GetColumnIndex(name string) (int, error) {
	for i, col := range s.Columns {
		if col.Name == name {
			return i, nil
		}
	}

	return -1, errors.New("column not found")
}
