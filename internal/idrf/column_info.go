package idrf

import "fmt"

// ColumnInfo represents DDL description of a single column in IDRF
type ColumnInfo struct {
	Name     string
	DataType DataType
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("ColumnInfo{%s, %s}", c.Name, c.DataType.String())
}

// NewColumn creates a new ColumnInfo without a foreign key while checking the arguments
func NewColumn(columnName string, dataType DataType) (*ColumnInfo, error) {
	if len(columnName) == 0 {
		return nil, fmt.Errorf("Column must have a name")
	}

	return &ColumnInfo{columnName, dataType}, nil
}
