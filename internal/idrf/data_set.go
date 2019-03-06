// Package idrf provides the structures for the Outflux Intermediate Data Reperesentation
// Format. These structures describe data and it's schema. The package also contains
// functions for safe initialization of the structures
package idrf

import (
	"fmt"
	"strings"
)

// DataSet represents DDL description of a single data set (table, measurement) in IDRF
type DataSet struct {
	DataSetName string
	Columns     []*Column
	TimeColumn  string
}

func (set *DataSet) String() string {
	return fmt.Sprintf("DataSet { Name: %s, Columns: %s, Time Column: %s }", set.DataSetName, set.Columns, set.TimeColumn)
}

// ColumnNamed returns the ColumnInfo for a column given it's name, or nil if no column
// with that name exists in the data set
func (set *DataSet) ColumnNamed(columnName string) *Column {
	for _, column := range set.Columns {
		if columnName == column.Name {
			return column
		}
	}

	return nil
}

// SchemaAndTable splits the data set identifier, "schema.table" -> "schema", "table"
func (set *DataSet) SchemaAndTable() (string, string) {
	dataSetNameParts := strings.SplitN(set.DataSetName, ".", 2)
	if len(dataSetNameParts) > 1 {
		return dataSetNameParts[0], dataSetNameParts[1]
	}

	return "", dataSetNameParts[0]
}

// NewDataSet creates a new instance of DataSet with checked arguments
func NewDataSet(dataSetName string, columns []*Column, timeColumn string) (*DataSet, error) {
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

	return &DataSet{dataSetName, columns, timeColumn}, nil
}
