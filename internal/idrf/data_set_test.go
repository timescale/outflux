package idrf

import (
	"fmt"
	"testing"
)

func TestNewDataSet(t *testing.T) {
	column, _ := NewColumn("Col 1", IDRFTimestamp)
	intColumn, _ := NewColumn("Col 1", IDRFInteger32)
	columns := []*Column{column}
	noTimestampTimeColumns := []*Column{intColumn}
	if _, error := NewDataSet("", columns, "Col 1"); error == nil {
		t.Error("Should not be able to create a data set with an empty name")
	}

	noColumns := []*Column{}
	if _, error := NewDataSet("Data Set", noColumns, ""); error == nil {
		t.Error("Should not be able to create a data set without columns")
	}

	duplicateColumns := []*Column{column, column}
	if _, error := NewDataSet("data set", duplicateColumns, "Col 1"); error == nil {
		t.Error("Should not be able to create a data set with duplicate columns")
	}

	if _, error := NewDataSet("Data Set", columns, "Col 2"); error == nil {
		t.Error("Data Set should not have been created, time column not in column set")
	}

	if _, error := NewDataSet("Data Set", noTimestampTimeColumns, "Col 1"); error == nil {
		t.Error("Data Set should not have been created, time column not a timestamp")
	}

	if _, error := NewDataSet("Data Set", columns, ""); error == nil {
		t.Error("data set should not have been created with time column empty")
	}

	dataSet, err := NewDataSet("Data Set", columns, "Col 1")
	if err != nil || dataSet == nil {
		t.Errorf("Data Set should have been created. Unexepcted err: %v", err)
	}

	if dataSet.DataSetName != "Data Set" {
		t.Errorf("Data set named %s, instead of %s", dataSet.DataSetName, "Data Set")
	}

	if len(dataSet.Columns) != 1 && dataSet.TimeColumn != "Col 1" {
		t.Errorf("data set columns not properly initialized")
	}

}
func TestColumnNamed(t *testing.T) {
	goodColumnName := "Col 1"
	badColumnName := "Col 2"

	expectedColumnType := IDRFTimestamp

	column, _ := NewColumn(goodColumnName, expectedColumnType)
	columns := []*Column{column}
	dataSet, _ := NewDataSet("Data Set", columns, "Col 1")

	goodColumn := dataSet.ColumnNamed(goodColumnName)
	if goodColumn == nil {
		t.Error("Column should have been found")
	}

	if dataSet.ColumnNamed(badColumnName) != nil {
		t.Error("Column name should not have been found")
	}

	if goodColumn.Name != goodColumnName || goodColumn.DataType != expectedColumnType {
		t.Error(
			fmt.Sprintf(
				"Found column was not good. Expected: name <%s> and type <%s>. Got: name <%s> and type <%s>",
				goodColumnName, expectedColumnType, goodColumn.Name, goodColumn.DataType,
			))
	}
}
