package catalog

import (
	"encoding/binary"

	"gobase/shared"
)

func DecodeTuple(schema *Schema, tuple shared.Tuple) []any {
	values := make([]any, len(schema.Columns))
	offset := 0

	for i, col := range schema.Columns {
		switch col.Type {
		case TypeInt:
			val := binary.LittleEndian.Uint32(tuple[offset:])
			values[i] = int(val)
			offset += 4
		case TypeSmallInt:
			val := binary.LittleEndian.Uint16(tuple[offset:])
			values[i] = int(val)
			offset += 2
		case TypeBoolean:
			values[i] = tuple[offset] != 0
			offset += 1
		case TypeVarchar:
			length := binary.LittleEndian.Uint16(tuple[offset:])
			offset += 2
			values[i] = string(tuple[offset : offset+int(length)])
			offset += int(length)
		}
	}

	return values
}
