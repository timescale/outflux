package ts

import (
	"fmt"
	"log"

	"github.com/timescale/outflux/internal/connections"
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
	hypertableDimensionsQueryTemplate = `SELECT column_name, column_type
                                         FROM _timescaledb_catalog.dimension d
              							 JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
										 WHERE h.schema_name = $1 AND h.table_name = $2
										 ORDER BY d.id ASC
										 LIMIT 1;`
	timescaleCreatedQuery         = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
	isNullableSignifyingValue     = "YES"
	installationMetadataTableName = "installation_metadata"
	telemetryMetadataTableName    = "telemetry_metadata"
	timescaleCatalogSchema        = "_timescaledb_catalog"
)

type tableFinder interface {
	tableExists(db connections.PgxWrap, schemaName, tableName string) (bool, error)
	metadataTableName(db connections.PgxWrap) (string, error)
}

type columnFinder interface {
	fetchTableColumns(db connections.PgxWrap, schemaName, tableName string) ([]*columnDesc, error)
}

type hypertableChecker interface {
	isHypertable(db connections.PgxWrap, schemaName, tableName string) (bool, error)
}

type hypertableDimensionExplorer interface {
	isTimePartitionedBy(db connections.PgxWrap, schema, table, timeColumn string) (bool, error)
}

type timescaleExistsChecker interface {
	timescaleExists(db connections.PgxWrap) (bool, error)
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

func (f *defaultTableFinder) tableExists(db connections.PgxWrap, schemaName, tableName string) (bool, error) {
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

func (f *defaultTableFinder) metadataTableName(db connections.PgxWrap) (string, error) {
	oldTableExists, err := f.tableExists(db, timescaleCatalogSchema, installationMetadataTableName)
	if err != nil {
		return "", err
	} else if oldTableExists {
		return installationMetadataTableName, err
	}
	newTableExists, err := f.tableExists(db, timescaleCatalogSchema, telemetryMetadataTableName)
	if err != nil {
		return "", err
	} else if newTableExists {
		return telemetryMetadataTableName, nil
	}

	return "", nil
}

func (f *defaultColumnFinder) fetchTableColumns(db connections.PgxWrap, schemaName, tableName string) ([]*columnDesc, error) {
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

func (f *defaultHyptertableChecker) isHypertable(db connections.PgxWrap, schemaName, tableName string) (bool, error) {
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

func (f *defaultHypertableDimensionExplorer) isTimePartitionedBy(db connections.PgxWrap, schema, table, timeColumn string) (bool, error) {
	if schema == "" {
		schema = "public"
	}

	rows, err := db.Query(hypertableDimensionsQueryTemplate, schema, table)
	if err != nil {
		return false, err
	}

	defer rows.Close()
	var partitioningColumn, dimensionType string

	if !rows.Next() {
		log.Printf("Table %s is not a hypertable", table)
		return false, nil
	}

	err = rows.Scan(&partitioningColumn, &dimensionType)
	if err != nil {
		return false, err
	}

	idrfDimType := pgTypeToIdrf(dimensionType)
	if idrfDimType != idrf.IDRFTimestamptz && idrfDimType != idrf.IDRFTimestamp {
		log.Printf("In order to import from influx, output hypertable should be partitioned by a timestamp, or timestamptz column")
		log.Printf("Table %s is partitioned by column %s of type %s", table, partitioningColumn, dimensionType)
		return false, nil
	}

	if partitioningColumn != timeColumn {
		return false, nil
	}

	return true, nil
}

func (d *defaultTimescaleExistsChecker) timescaleExists(db connections.PgxWrap) (bool, error) {
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
