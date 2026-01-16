package catalog

import (
	"bytes"
	"encoding/binary"

	"gobase/shared"
)

func EncodeTuple(schema *Schema, values []any) shared.Tuple {
	buffer := new(bytes.Buffer)

	for i, col := range schema.Columns {
		switch col.Type {
		case TypeInt:
			val := values[i].(int)
			binary.Write(buffer, binary.LittleEndian, int32(val))
		case TypeSmallInt:
			val := values[i].(int)
			binary.Write(buffer, binary.LittleEndian, int16(val))
		case TypeBoolean:
			val := values[i].(bool)
			if val {
				buffer.WriteByte(1)
			} else {
				buffer.WriteByte(0)
			}
		case TypeVarchar:
			val := values[i].(string)
			binary.Write(buffer, binary.LittleEndian, uint16(len(val)))
			buffer.WriteString(val)
		default:
			continue
		}
	}

	return buffer.Bytes()
}
