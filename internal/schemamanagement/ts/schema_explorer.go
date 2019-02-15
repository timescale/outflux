package ts

import (
	"fmt"
	"github.com/jackc/pgx"

	"github.com/lib/pq"
	"github.com/timescale/outflux/internal/idrf"
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
	hypertableDimensionsQueryTemplate = "SELECT partitioning_columns, partitioning_column_types FROM chunk_relation_size_pretty('%s') limit 1;"
	timescaleCreatedQuery             = "SELECT EXISTS (SELECT 1 FROM from pg_extension WHERE extname = 'timescaledb')"
	isNullableSignifyingValue         = "YES"
)

type tableFinder interface {
	tableExists(db *pgx.Conn, schemaName, tableName string) (bool, error)
}

type columnFinder interface {
	fetchTableColumns(db *pgx.Conn, schemaName, tableName string) ([]*columnDesc, error)
}

type hypertableChecker interface {
	isHypertable(db *pgx.Conn, schemaName, tableName string) (bool, error)
}

type hypertableDimensionExplorer interface {
	isTimePartitionedBy(db *pgx.Conn, schema, table, timeColumn string) (bool, error)
}

type timescaleExistsChecker interface {
	timescaleExists(db *pgx.Conn) (bool, error)
}

type schemaExplorer interface {
	tableFinder
	columnFinder
	hypertableChecker
	hypertableDimensionExplorer
	timescaleExistsChecker
}

type defaultTableFinder struct{}
type defaultColumnFinder struct{}
type defaultHyptertableChecker struct{}
type defaultHypertableDimensionExplorer struct{}
type defaultTimescaleExistsChecker struct{}
type defaultSchemaExplorer struct {
	tableFinder
	columnFinder
	hypertableChecker
	hypertableDimensionExplorer
	timescaleExistsChecker
}

func newSchemaExplorer() schemaExplorer {
	return &defaultSchemaExplorer{
		&defaultTableFinder{},
		&defaultColumnFinder{},
		&defaultHyptertableChecker{},
		&defaultHypertableDimensionExplorer{},
		&defaultTimescaleExistsChecker{},
	}
}

func newSchemaExplorerWith(
	tblFinder tableFinder,
	colFinder columnFinder,
	hyperChecker hypertableChecker,
	dimExplorer hypertableDimensionExplorer,
	tsChecker timescaleExistsChecker) schemaExplorer {
	return &defaultSchemaExplorer{
		tblFinder,
		colFinder,
		hyperChecker,
		dimExplorer,
		tsChecker,
	}
}

func (f *defaultTableFinder) tableExists(db *pgx.Conn, schemaName, tableName string) (bool, error) {
	if schemaName == "" {
		schemaName = "public"
	}
	rows, err := db.Query(tableExistsQueryTemplate, schemaName, tableName)
	if err != nil {
		return true, err
	}
	defer rows.Close()
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

func (f *defaultColumnFinder) fetchTableColumns(db *pgx.Conn, schemaName, tableName string) ([]*columnDesc, error) {
	if schemaName == "" {
		schemaName = "public"
	}

	rows, err := db.Query(tableColumnsQueryTemplate, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (f *defaultHyptertableChecker) isHypertable(db *pgx.Conn, schemaName, tableName string) (bool, error) {
	if schemaName == "" {
		schemaName = "public"
	}

	rows, err := db.Query(isHypertableQueryTemplate, schemaName, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()

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

func (f *defaultHypertableDimensionExplorer) isTimePartitionedBy(db *pgx.Conn, schema, table, timeColumn string) (bool, error) {
	if schema == "" {
		schema = "public"
	}

	query := fmt.Sprintf(hypertableDimensionsQueryTemplate, schema+"."+table)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}

	defer rows.Close()
	var dimensions, dimensionTypes pq.StringArray

	if !rows.Next() {
		return false, fmt.Errorf("couldn't extract result from postgres response")
	}

	err = rows.Scan(dimensions, dimensionTypes)
	if err != nil {
		return false, err
	}

	if dimensions == nil || len(dimensions) < 1 {
		return false, fmt.Errorf("hypertable didn't have no partitioning dimensions")
	}

	firstDimType := pgTypeToIdrf(dimensionTypes[0])
	if firstDimType != idrf.IDRFTimestamptz && firstDimType != idrf.IDRFTimestamp {
		return false, nil
	}

	if dimensions[0] != timeColumn {
		return false, nil
	}

	return true, nil
}

func (d *defaultTimescaleExistsChecker) timescaleExists(db *pgx.Conn) (bool, error) {
	rows, err := db.Query(timescaleCreatedQuery)
	if err != nil {
		return false, err
	}

	defer rows.Close()
	exists := false
	if !rows.Next() {
		return false, fmt.Errorf("couldn't extract result from postgres response")
	}

	err = rows.Scan(&exists)
	return exists, err
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
