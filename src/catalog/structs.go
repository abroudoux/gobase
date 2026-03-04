package catalog

type Column struct {
	Name     string
	Type     ColumnType
	Size     uint16
	Nullable bool
}

type Schema struct {
	Columns []Column
}

func NewSchema(columns []Column) *Schema {
	return &Schema{Columns: columns}
}
