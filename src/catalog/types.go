package catalog

type ColumnType uint8

const (
	TypeInt ColumnType = iota
	TypeSmallInt
	TypeBoolean
	TypeVarchar
)
