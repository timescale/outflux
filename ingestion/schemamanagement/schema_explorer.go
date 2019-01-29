package schemamanagement

import (
	"database/sql"
	"fmt"
)

const (
	tableExistsQueryTemplate  = "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE  schemaname = $1 AND tablename = $2)"
	tableColumnsQueryTemplate = `SELECT column_name, data_type, is_nullable
	                             FROM information_schema.columns
								 WHERE table_schema = $1 AND table_name = $2;`
	isHypertableQueryTemplate = `SELECT EXISTS (
		    						SELECT 1 
								 	FROM timescaledb_information.hypertable 
									 WHERE  table_schema = $1 AND table_name=$2)`
	isNullableSignifyingValue = "YES"
)

type tableFinder interface {
	tableExists(db *sql.DB, schemaName, tableName string) (bool, error)
}

type columnFinder interface {
	fetchTableColumns(db *sql.DB, schemaName, tableName string) ([]*columnDesc, error)
}

type hypertableChecker interface {
	isHypertable(db *sql.DB, schemaName, tableName string) (bool, error)
}

type schemaExplorer interface {
	tableFinder
	columnFinder
	hypertableChecker
}

type defaultTableFinder struct{}
type defaultColumnFinder struct{}
type defaultHyptertableChecker struct{}

type defaultSchemaExplorer struct {
	tableFinder
	columnFinder
	hypertableChecker
}

func newSchemaExplorer() schemaExplorer {
	return &defaultSchemaExplorer{
		&defaultTableFinder{}, &defaultColumnFinder{}, &defaultHyptertableChecker{},
	}
}

func newSchemaExplorerWith(tblFinder tableFinder, colFinder columnFinder, hyperChecker hypertableChecker) schemaExplorer {
	return &defaultSchemaExplorer{
		tblFinder, colFinder, hyperChecker,
	}
}

func (f *defaultTableFinder) tableExists(db *sql.DB, schemaName, tableName string) (bool, error) {
	if schemaName == "" {
		schemaName = "public"
	}
	rows, err := db.Query(tableExistsQueryTemplate, schemaName, tableName)
	if err != nil {
		return true, err
	}

	exists := false
	if !rows.Next() {
		return true, fmt.Errorf("couldn't extract result from postgres response")
	}
	err = rows.Scan(&exists)
	if err != nil {
		return true, err
	}

	return exists, nil
}

func (f *defaultColumnFinder) fetchTableColumns(db *sql.DB, schemaName, tableName string) ([]*columnDesc, error) {
	if schemaName == "" {
		schemaName = "public"
	}

	rows, err := db.Query(tableColumnsQueryTemplate, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	first := &node{}
	prev := first
	numColumns := 0
	for rows.Next() {
		desc := &columnDesc{}
		err = rows.Scan(&desc.columnName, &desc.dataType, &desc.isNullable)
		if err != nil {
			return nil, err
		}
		numColumns++
		current := &node{}
		current.value = desc
		prev.next = current
		prev = current
	}

	columnsArray := make([]*columnDesc, numColumns)
	i := 0
	for {
		if first.next == nil {
			break
		}

		first = first.next
		columnsArray[i] = first.value.(*columnDesc)
		i++
	}

	return columnsArray, nil
}

func (f *defaultHyptertableChecker) isHypertable(db *sql.DB, schemaName, tableName string) (bool, error) {
	if schemaName == "" {
		schemaName = "public"
	}

	rows, err := db.Query(isHypertableQueryTemplate, schemaName, tableName)
	if err != nil {
		return false, err
	}

	exists := false
	if !rows.Next() {
		return true, fmt.Errorf("couldn extract result from postgres response")
	}
	err = rows.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

type columnDesc struct {
	columnName string
	dataType   string
	isNullable string
}

func (col *columnDesc) isColumnNullable() bool {
	return col.isNullable == isNullableSignifyingValue
}

type node struct {
	value interface{}
	next  *node
}
