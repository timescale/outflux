package intermediatedataformat

import "fmt"

// DataSetInfo represents DDL description of a single data set (table, measurement) in IDRF
type DataSetInfo struct {
	dataSetName string
	columns     []ColumnInfo
}

// NewDataSet creates a new instance of DataSetInfo with checked arguments
func NewDataSet(dataSetName string, columns []ColumnInfo) (*DataSetInfo, *IdrfError) {
	if len(dataSetName) == 0 {
		return nil, &IdrfError{"Data set name can't be empty"}
	}

	if len(columns) == 0 {
		return nil, &IdrfError{"Data set must have at least one column"}
	}

	columnNameCount := make(map[string]bool)
	for _, columnInfo := range columns {
		_, columnNameExists := columnNameCount[columnInfo.name]
		if columnNameExists {
			return nil, &IdrfError{"Duplicate column names found"}
		}

		columnNameCount[columnInfo.name] = true
	}

	return &DataSetInfo{dataSetName, columns}, nil
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

// ColumnInfo represents DDL description of a single column in IDRF
type ColumnInfo struct {
	name       string
	dataType   DataType
	foreignKey *ForeignKeyDescription
}

// NewColumnWithFK creates a new ColumnInfo with a foreign key while checking the arguments
func NewColumnWithFK(columnName string, dataType DataType, foreignKey ForeignKeyDescription) (*ColumnInfo, *IdrfError) {
	column, error := NewColumn(columnName, dataType)
	if error != nil {
		return nil, error
	}

	foreignColumn := foreignKey.dataSet.ColumnNamed(foreignKey.columnName)
	if foreignColumn == nil {
		return nil, &IdrfError{"Foreign key is invalid, column not found in data set"}
	}

	if foreignColumn.dataType != dataType {
		return nil, &IdrfError{"Foreign key remote column is of different type"}
	}

	column.foreignKey = &foreignKey
	return column, nil
}

// NewColumn creates a new ColumnInfo without a foreign key while checking the arguments
func NewColumn(columnName string, dataType DataType) (*ColumnInfo, *IdrfError) {
	if len(columnName) == 0 {
		return nil, &IdrfError{"Column must have a name"}
	}

	return &ColumnInfo{columnName, dataType, nil}, nil
}

// DataType Supported data types in the Intermediate Data Representation Format
type DataType int

// Available values for IDRF DataType enum
const (
	IDRFInteger  DataType = 0
	IDRFFloating DataType = 1
	IDRFString   DataType = 2
	IDFRBoolean  DataType = 3
)

// ForeignKeyDescription describes a foreign key relationship to a IDRF data set's column
type ForeignKeyDescription struct {
	dataSet    DataSetInfo
	columnName string
}

// NewForeignKey creates a new instance of ForeignKeyDescription with checked arguments
func NewForeignKey(dataSet DataSetInfo, columnName string) (*ForeignKeyDescription, *IdrfError) {
	columnExistsInDataSet := dataSet.ColumnNamed(columnName) != nil
	if !columnExistsInDataSet {
		return nil, &IdrfError{fmt.Sprintf("Column %s not part of data set %s", columnName, dataSet.dataSetName)}
	}

	return &ForeignKeyDescription{dataSet, columnName}, nil
}

// IdrfError if anything wrong happens in an IDRF function it returns this type of error.
type IdrfError struct {
	message string
}

func (error *IdrfError) Error() string {
	return error.message
}
