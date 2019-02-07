package schemamanagement

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/timescale/outflux/idrf"
)

const (
	createTableQueryTemplate      = "CREATE TABLE %s(%s)"
	columnDefTemplate             = "%s %s"
	createHypertableQueryTemplate = "SELECT create_hypertable('%s', '%s');"
)

type tableCreator interface {
	Create(dbConn *sql.DB, schema string, info *idrf.DataSetInfo) error
}

func newTableCreator() tableCreator {
	return &defaultTableCreator{}
}

type defaultTableCreator struct{}

func (d *defaultTableCreator) Create(dbConn *sql.DB, schema string, info *idrf.DataSetInfo) error {
	tableName := info.DataSetName
	if schema != "" {
		tableName = schema + "." + tableName
	}

	query := dataSetToSQLTableDef(tableName, info)

	rows, err := dbConn.Query(query)
	if err != nil {
		return err
	}
	rows.Close()
	hypertableQuery := fmt.Sprintf(createHypertableQueryTemplate, tableName, info.TimeColumn)
	rows, err = dbConn.Query(hypertableQuery)
	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

func dataSetToSQLTableDef(tableName string, dataSet *idrf.DataSetInfo) string {
	columnDefinitions := make([]string, len(dataSet.Columns))
	for i, column := range dataSet.Columns {
		dataType := idrfToPgType(column.DataType)
		columnDefinitions[i] = fmt.Sprintf(columnDefTemplate, column.Name, dataType)
	}

	columnsString := strings.Join(columnDefinitions, ", ")

	return fmt.Sprintf(createTableQueryTemplate, tableName, columnsString)
}

func idrfToPgType(dataType idrf.DataType) string {
	switch dataType {
	case idrf.IDRFBoolean:
		return "BOOLEAN"
	case idrf.IDRFDouble:
		return "FLOAT"
	case idrf.IDRFInteger32:
		return "INTEGER"
	case idrf.IDRFString:
		return "TEXT"
	case idrf.IDRFTimestamp:
		return "TIMESTAMP"
	case idrf.IDRFTimestamptz:
		return "TIMESTAMPTZ"
	case idrf.IDRFInteger64:
		return "BIGINT"
	case idrf.IDRFSingle:
		return "FLOAT"
	default:
		panic("Unexpected value")
	}
}
