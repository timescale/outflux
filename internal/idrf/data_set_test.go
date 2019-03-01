package idrf

import (
	"fmt"
	"testing"
)

func TestNewForeignKey(t *testing.T) {
	// If column doesn't exist in data set error is returned
	dataSetNoColumns := DataSetInfo{DataSetName: "ds", Columns: []*ColumnInfo{}}
	wrongColumnName := "Wrong Column"
	goodColumnName := "Column 1"

	foreignKey, error := NewForeignKey(&dataSetNoColumns, wrongColumnName)
	if error == nil || foreignKey != nil {
		t.Error("Error should have been returned because column is not in data set")
	}

	dataSetWithColumns := DataSetInfo{DataSetName: "ds", Columns: []*ColumnInfo{
		&ColumnInfo{Name: goodColumnName, DataType: IDRFTimestamp},
	}}

	foreignKey, error = NewForeignKey(&dataSetWithColumns, wrongColumnName)
	if error == nil || foreignKey != nil {
		t.Error("Error should have been returned because column is not in data set")
	}

	foreignKey, error = NewForeignKey(&dataSetWithColumns, goodColumnName)
	if error != nil || foreignKey == nil {
		t.Error("Error should not have been returned, column is in the data set")
	}
}

func TestNewColumn(t *testing.T) {
	if _, err := NewColumn("", IDRFDouble); err == nil {
		t.Error("Empty column name should not be allowed")
	}

	if col, err := NewColumn("Col Name", IDRFBoolean); col == nil || err != nil {
		t.Error("Column should have been created")
	}
}

func TestNewColumnWithFK(t *testing.T) {
	foreignColumn := "Col1"
	goodDataSet := DataSetInfo{
		DataSetName: "DSName",
		Columns:     []*ColumnInfo{&ColumnInfo{Name: foreignColumn, DataType: IDRFBoolean}},
	}
	goodForeignKey, err := NewForeignKey(&goodDataSet, foreignColumn)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if _, err := NewColumnWithFK("", IDRFBoolean, goodForeignKey); err == nil {
		t.Error("Empty column name should not be allowed")
	}

	if _, err := NewColumnWithFK("Col 2", IDRFDouble, goodForeignKey); err == nil {
		t.Error("Error because column type is not the same as the referenced foreign column")
	}

	if column, err := NewColumnWithFK("Col 2", IDRFBoolean, goodForeignKey); err != nil || column == nil {
		t.Error("Should have created the column")
	}
}

func TestNewDataSet(t *testing.T) {
	column, _ := NewColumn("Col 1", IDRFTimestamp)
	intColumn, _ := NewColumn("Col 1", IDRFTimestamp)
	columns := []*ColumnInfo{column}
	noTimestampTimeColumns := []*ColumnInfo{intColumn}
	if _, error := NewDataSet("", columns, "Col 1"); error == nil {
		t.Error("Should not be able to create a data set with an empty name")
	}

	noColumns := []*ColumnInfo{}
	if _, error := NewDataSet("Data Set", noColumns, ""); error == nil {
		t.Error("Should not be able to create a data set without columns")
	}

	duplicateColumns := []*ColumnInfo{column, column}
	if _, error := NewDataSet("data set", duplicateColumns, "Col 1"); error == nil {
		t.Error("Should not be able to create a data set with duplicate columns")
	}

	if dataSet, error := NewDataSet("Data Set", columns, "Col 2"); error == nil || dataSet != nil {
		t.Error("Data Set should not have been created, time column not in column set")
	}

	if dataSet, error := NewDataSet("Data Set", noTimestampTimeColumns, "Col 1"); error != nil || dataSet == nil {
		t.Error("Data Set should not have been created, time column not a timestamp")
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
	columns := []*ColumnInfo{column}
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
