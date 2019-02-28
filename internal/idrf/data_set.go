// Package idrf provides the structures for the Outflux Intermediate Data Reperesentation
// Format. These structures describe data and it's schema. The package also contains
// functions for safe initialization of the structures
package idrf

import (
	"fmt"
	"strings"
)

// DataSetInfo represents DDL description of a single data set (table, measurement) in IDRF
type DataSetInfo struct {
	DataSetName string
	Columns     []*ColumnInfo
	TimeColumn  string
}

func (set *DataSetInfo) String() string {
	return fmt.Sprintf("DataSetInfo { dataSetName: %s, columns: %s, time column: %s }", set.DataSetName, set.Columns, set.TimeColumn)
}

// ColumnNamed returns the ColumnInfo for a column given it's name, or nil if no column
// with that name exists in the data set
func (set DataSetInfo) ColumnNamed(columnName string) *ColumnInfo {
	for _, column := range set.Columns {
		if columnName == column.Name {
			return column
		}
	}

	return nil
}

// SchemaAndTable splits the data set identifier, "schema.table" -> "schema", "table"
func (set DataSetInfo) SchemaAndTable() (string, string) {
	dataSetNameParts := strings.SplitN(set.DataSetName, ".", 2)
	if len(dataSetNameParts) > 1 {
		return dataSetNameParts[0], dataSetNameParts[1]
	}

	return "", dataSetNameParts[0]
}

// NewDataSet creates a new instance of DataSetInfo with checked arguments
func NewDataSet(dataSetName string, columns []*ColumnInfo, timeColumn string) (*DataSetInfo, error) {
	if len(dataSetName) == 0 {
		return nil, fmt.Errorf("data set name can't be empty")
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("data set must have at least one column")
	}

	if timeColumn == "" {
		return nil, fmt.Errorf("data set must have a time column specified")
	}

	columnSet := make(map[string]bool)
	timeColumnDefined := false
	for _, columnInfo := range columns {
		if _, exists := columnSet[columnInfo.Name]; exists {
			return nil, fmt.Errorf("duplicate column names found: %s", columnInfo.Name)
		}

		columnSet[columnInfo.Name] = true
		if columnInfo.Name == timeColumn {
			if columnInfo.DataType != IDRFTimestamp && columnInfo.DataType != IDRFTimestamptz {
				return nil, fmt.Errorf("time column '%s', is not of a Timestamp(tz) type", timeColumn)
			}

			timeColumnDefined = true
		}
	}

	if !timeColumnDefined {
		return nil, fmt.Errorf("time column %s, not found in columns array", timeColumn)
	}

	return &DataSetInfo{dataSetName, columns, timeColumn}, nil
}

// ColumnInfo represents DDL description of a single column in IDRF
type ColumnInfo struct {
	Name       string
	DataType   DataType
	ForeignKey *ForeignKeyDescription
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("ColumnInfo { name: %s, dataType: %s, fk: %v}", c.Name, c.DataType.String(), c.ForeignKey)
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

	if foreignColumn.DataType != dataType {
		return nil, fmt.Errorf("Foreign key remote column is of different type")
	}

	column.ForeignKey = foreignKey
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
	IDRFInteger32 DataType = iota + 1
	IDRFInteger64
	IDRFDouble
	IDRFSingle
	IDRFString
	IDRFBoolean
	IDRFTimestamptz
	IDRFTimestamp
	IDRFUnknown
)

func (d DataType) String() string {
	switch d {
	case IDRFBoolean:
		return "IDRFBoolean"
	case IDRFDouble:
		return "IDRFDouble"
	case IDRFInteger32:
		return "IDRFInteger32"
	case IDRFString:
		return "IDRFString"
	case IDRFTimestamp:
		return "IDRFTimestamp"
	case IDRFTimestamptz:
		return "IDRFTimestamptz"
	case IDRFInteger64:
		return "IDRFInteger64"
	case IDRFSingle:
		return "IDRFSingle"
	case IDRFUnknown:
		return "IDRFUnknown"
	default:
		panic("Unexpected value")
	}
}

// CanFitInto returns true if this data type can be safely cast to the other data type
func (d DataType) CanFitInto(other DataType) bool {
	if d == other {
		return true
	}

	if d == IDRFInteger32 {
		return other == IDRFSingle || other == IDRFDouble || other == IDRFInteger64
	}

	if d == IDRFInteger64 {
		return other == IDRFDouble
	}

	if d == IDRFTimestamp {
		return other == IDRFTimestamptz
	}

	return false
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
		return nil, fmt.Errorf("Column %s not part of data set %s", columnName, dataSet.DataSetName)
	}

	return &ForeignKeyDescription{dataSet, columnName}, nil
}
