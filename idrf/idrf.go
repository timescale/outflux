// Package idrf provides the structures for the Outflux Intermediate Data Reperesentation
// Format. These structures describe data and it's schema. The package also contains
// functions for safe initialization of the structures
package idrf

import "fmt"

// DataSetInfo represents DDL description of a single data set (table, measurement) in IDRF
type DataSetInfo struct {
	dataSetName string
	columns     []ColumnInfo
}

func (set *DataSetInfo) String() string {
	return fmt.Sprintf("DataSetInfo { dataSetName: %s, columns: %s }", set.dataSetName, set.columns)
}

// ColumnNamed returns the ColumnInfo for a column given it's name, or nil if no column
// with that name exists in the data set
func (set DataSetInfo) ColumnNamed(columnName string) *ColumnInfo {
	for _, column := range set.columns {
		if columnName == column.name {
			return &column
		}
	}

	return nil
}

// NewDataSet creates a new instance of DataSetInfo with checked arguments
func NewDataSet(dataSetName string, columns []ColumnInfo) (*DataSetInfo, error) {
	if len(dataSetName) == 0 {
		return nil, fmt.Errorf("Data set name can't be empty")
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("Data set must have at least one column")
	}

	columnSet := make(map[string]bool)
	for _, columnInfo := range columns {
		if _, exists := columnSet[columnInfo.name]; exists {
			return nil, fmt.Errorf("Duplicate column names found")
		}

		columnSet[columnInfo.name] = true
	}

	return &DataSetInfo{dataSetName, columns}, nil
}

// ColumnInfo represents DDL description of a single column in IDRF
type ColumnInfo struct {
	name       string
	dataType   DataType
	foreignKey *ForeignKeyDescription
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("ColumnInfo { name: %s, dataType: %s, fk: %v}", c.name, c.dataType.String(), c.foreignKey)
}

// NewColumnWithFK creates a new ColumnInfo with a foreign key while checking the arguments
func NewColumnWithFK(columnName string, dataType DataType, foreignKey *ForeignKeyDescription) (*ColumnInfo, error) {
	column, error := NewColumn(columnName, dataType)
	if error != nil {
		return nil, error
	}

	foreignColumn := foreignKey.dataSet.ColumnNamed(foreignKey.columnName)
	if foreignColumn == nil {
		return nil, fmt.Errorf("Foreign key is invalid, column not found in data set")
	}

	if foreignColumn.dataType != dataType {
		return nil, fmt.Errorf("Foreign key remote column is of different type")
	}

	column.foreignKey = foreignKey
	return column, nil
}

// NewColumn creates a new ColumnInfo without a foreign key while checking the arguments
func NewColumn(columnName string, dataType DataType) (*ColumnInfo, error) {
	if len(columnName) == 0 {
		return nil, fmt.Errorf("Column must have a name")
	}

	return &ColumnInfo{columnName, dataType, nil}, nil
}

// DataType Supported data types in the Intermediate Data Representation Format
type DataType int

// Available values for IDRF DataType enum
const (
	IDRFInteger DataType = iota + 1
	IDRFFloating
	IDRFString
	IDRFBoolean
	IDRFTimestamp
)

func (d DataType) String() string {
	switch d {
	case IDRFBoolean:
		return "IDRFBoolean"
	case IDRFFloating:
		return "IDRFFloating"
	case IDRFInteger:
		return "IDRFInteger"
	case IDRFString:
		return "IDRFString"
	case IDRFTimestamp:
		return "IDRFTimestamp"
	default:
		panic("Unexpected value")
	}
}

// ForeignKeyDescription describes a foreign key relationship to a IDRF data set's column
type ForeignKeyDescription struct {
	dataSet    *DataSetInfo
	columnName string
}

// NewForeignKey creates a new instance of ForeignKeyDescription with checked arguments
func NewForeignKey(dataSet *DataSetInfo, columnName string) (*ForeignKeyDescription, error) {
	columnExistsInDataSet := dataSet.ColumnNamed(columnName) != nil
	if !columnExistsInDataSet {
		return nil, fmt.Errorf("Column %s not part of data set %s", columnName, dataSet.dataSetName)
	}

	return &ForeignKeyDescription{dataSet, columnName}, nil
}
