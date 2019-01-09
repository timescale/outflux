package idrf

import (
	"fmt"
	"testing"
)

func TestNewForeignKey(t *testing.T) {
	// If column doesn't exist in data set error is returned
	dataSetNoColumns := DataSetInfo{dataSetName: "ds", columns: []ColumnInfo{}}
	wrongColumnName := "Wrong Column"
	goodColumnName := "Column 1"

	foreignKey, error := NewForeignKey(dataSetNoColumns, wrongColumnName)
	if error == nil || foreignKey != nil {
		t.Error("Error should have been returned because column is not in data set")
	}

	dataSetWithColumns := DataSetInfo{dataSetName: "ds", columns: []ColumnInfo{
		ColumnInfo{name: goodColumnName, dataType: IDRFInteger},
	}}

	foreignKey, error = NewForeignKey(dataSetWithColumns, wrongColumnName)
	if error == nil || foreignKey != nil {
		t.Error("Error should have been returned because column is not in data set")
	}

	foreignKey, error = NewForeignKey(dataSetWithColumns, goodColumnName)
	if error != nil || foreignKey == nil {
		t.Error("Error should not have been returned, column is in the data set")
	}
}

func TestNewColumn(t *testing.T) {
	if _, err := NewColumn("", IDRFFloating); err == nil {
		t.Error("Empty column name should not be allowed")
	}

	if col, err := NewColumn("Col Name", IDRFFloating); col == nil || err != nil {
		t.Error("Column should have been created")
	}
}

func TestNewColumnWithFK(t *testing.T) {
	foreignColumn := "Col1"
	goodDataSet := DataSetInfo{
		dataSetName: "DSName",
		columns:     []ColumnInfo{ColumnInfo{name: foreignColumn, dataType: IDRFFloating}},
	}
	goodForeignKey, err := NewForeignKey(goodDataSet, foreignColumn)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if _, err := NewColumnWithFK("", IDRFFloating, goodForeignKey); err == nil {
		t.Error("Empty column name should not be allowed")
	}

	if _, err := NewColumnWithFK("Col 2", IDRFInteger, goodForeignKey); err == nil {
		t.Error("Error because column type is not the same as the referenced foreign column")
	}

	if column, err := NewColumnWithFK("Col 2", IDRFFloating, goodForeignKey); err != nil || column == nil {
		t.Error("Should have created the column")
	}
}

func TestNewDataSet(t *testing.T) {
	column, _ := NewColumn("Col 1", IDRFFloating)
	columns := []ColumnInfo{*column}
	if _, error := NewDataSet("", columns); error == nil {
		t.Error("Should not be able to create a data set with an empty name")
	}

	noColumns := []ColumnInfo{}
	if _, error := NewDataSet("Data Set", noColumns); error == nil {
		t.Error("Should not be able to create a data set without columns")
	}

	duplicateColumns := []ColumnInfo{*column, *column}
	if _, error := NewDataSet("data set", duplicateColumns); error == nil {
		t.Error("Should not be able to create a data set with duplicate columns")
	}

	if dataSet, error := NewDataSet("Data Set", columns); error != nil || dataSet == nil {
		t.Error("Data Set should have been created")
	}
}
func TestColumnNamed(t *testing.T) {
	goodColumnName := "Col 1"
	badColumnName := "Col 2"

	expectedColumnType := IDRFFloating

	column, _ := NewColumn(goodColumnName, expectedColumnType)
	columns := []ColumnInfo{*column}
	dataSet, _ := NewDataSet("Data Set", columns)

	goodColumn := dataSet.ColumnNamed(goodColumnName)
	if goodColumn == nil {
		t.Error("Column should have been found")
	}

	if dataSet.ColumnNamed(badColumnName) != nil {
		t.Error("Column name should not have been found")
	}

	if goodColumn.name != goodColumnName || goodColumn.dataType != expectedColumnType {
		t.Error(
			fmt.Sprintf(
				"Found column was not good. Expected: name <%s> and type <%s>. Got: name <%s> and type <%s>",
				goodColumnName, expectedColumnType, goodColumn.name, goodColumn.dataType,
			))
	}
}
