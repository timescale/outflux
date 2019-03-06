package idrf

import "fmt"

// Column represents DDL description of a single column in IDRF
type Column struct {
	Name     string
	DataType DataType
}

func (c Column) String() string {
	return fmt.Sprintf("Column { Name: %s, DataType: %s}", c.Name, c.DataType.String())
}

// NewColumn creates a new ColumnInfo without a foreign key while checking the arguments
func NewColumn(columnName string, dataType DataType) (*Column, error) {
	if len(columnName) == 0 {
		return nil, fmt.Errorf("Column must have a name")
	}

	return &Column{columnName, dataType}, nil
}
