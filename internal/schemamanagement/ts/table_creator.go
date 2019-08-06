package ts

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/idrf"
)

const (
	createTableQueryTemplate               = `CREATE TABLE %s(%s)`
	columnDefTemplate                      = `"%s" %s`
	tableNameTemplate                      = `"%s"`
	timeColTemplate                        = tableNameTemplate
	tableNameWithSchemaTemplate            = `"%s"."%s"`
	createHTWithChunkIntervalQueryTemplate = `SELECT create_hypertable('%s', '%s', chunk_time_interval => interval '%s');`
	createHTQueryTemplate                  = `SELECT create_hypertable('%s', '%s');`
	createTimescaleExtensionQuery          = "CREATE EXTENSION IF NOT EXISTS timescaledb"
	metadataKey                            = "outflux_last_usage"
	getMetadataTemplate                    = `SELECT EXISTS (SELECT 1 FROM "%s"."%s" WHERE key = $1)`
	insertMetadataTemplate                 = "INSERT INTO %s.%s VALUES($1, $2)"
	updateMetadataTemplate                 = "UPDATE %s.%s SET value=$1 WHERE key=$2"
)

type tableCreator interface {
	CreateTable(connections.PgxWrap, *idrf.DataSet) error
	CreateHypertable(connections.PgxWrap, *idrf.DataSet) error
	CreateTimescaleExtension(connections.PgxWrap) error
	UpdateMetadata(db connections.PgxWrap, metadataTableName string) error
}

func newTableCreator(schema, chunkTimeInterval string) tableCreator {
	return &defaultTableCreator{schema: schema, chunkTimeInterval: chunkTimeInterval}
}

type defaultTableCreator struct {
	schema            string
	chunkTimeInterval string
}

func (d *defaultTableCreator) CreateTable(dbConn connections.PgxWrap, info *idrf.DataSet) error {
	query := dataSetToSQLTableDef(d.schema, info)
	log.Printf("Creating table with:\n %s", query)

	if _, err := dbConn.Exec(query); err != nil {
		return err
	}

	if err := d.CreateTimescaleExtension(dbConn); err != nil {
		return err
	}

	return d.CreateHypertable(dbConn, info)
}

func (d *defaultTableCreator) CreateHypertable(dbConn connections.PgxWrap, info *idrf.DataSet) error {
	var hypertableName string

	if d.schema != "" {
		hypertableName = fmt.Sprintf(tableNameWithSchemaTemplate, d.schema, info.DataSetName)
	} else {
		hypertableName = fmt.Sprintf(tableNameTemplate, info.DataSetName)
	}

	var hypertableQuery string
	if d.chunkTimeInterval != "" {
		hypertableQuery = fmt.Sprintf(createHTWithChunkIntervalQueryTemplate, hypertableName, info.TimeColumn, d.chunkTimeInterval)
	} else {
		hypertableQuery = fmt.Sprintf(createHTQueryTemplate, hypertableName, info.TimeColumn)
	}

	log.Printf("Creating hypertable with: %s", hypertableQuery)
	_, err := dbConn.Exec(hypertableQuery)
	return err
}

func (d *defaultTableCreator) CreateTimescaleExtension(dbConn connections.PgxWrap) error {
	log.Printf("Preparing TimescaleDB extension:\n%s", createTimescaleExtensionQuery)
	_, err := dbConn.Exec(createTimescaleExtensionQuery)
	return err
}

func (d *defaultTableCreator) UpdateMetadata(dbConn connections.PgxWrap, metadataTableName string) error {
	log.Printf("Updating Timescale metadata")
	metadataQuery := fmt.Sprintf(getMetadataTemplate, timescaleCatalogSchema, metadataTableName)
	rows, err := dbConn.Query(metadataQuery, metadataKey)
	if err != nil {
		return fmt.Errorf("could not check if Outflux metadata already exists. %v", err)
	}
	exists := false
	if !rows.Next() {
		rows.Close()
		return fmt.Errorf("could not check if Outflux metadata already exists. %v", err)
	}
	err = rows.Scan(&exists)
	if err != nil {
		rows.Close()
		return fmt.Errorf("could not check if Outflux installation metadata already exists. %v", err)
	}

	rows.Close()
	currentDateTime := time.Now().Format(time.RFC3339)
	if exists {
		updateMetadata := fmt.Sprintf(updateMetadataTemplate, timescaleCatalogSchema, metadataTableName)
		_, err = dbConn.Exec(updateMetadata, currentDateTime, metadataKey)
	} else {
		insertMetadata := fmt.Sprintf(insertMetadataTemplate, timescaleCatalogSchema, metadataTableName)
		_, err = dbConn.Exec(insertMetadata, metadataKey, currentDateTime)
	}
	return err
}

func dataSetToSQLTableDef(schema string, dataSet *idrf.DataSet) string {
	columnDefinitions := make([]string, len(dataSet.Columns))
	for i, column := range dataSet.Columns {
		dataType := idrfToPgType(column.DataType)
		columnDefinitions[i] = fmt.Sprintf(columnDefTemplate, column.Name, dataType)
	}

	columnsString := strings.Join(columnDefinitions, ", ")

	var tableName string
	if schema != "" {
		tableName = fmt.Sprintf(tableNameWithSchemaTemplate, schema, dataSet.DataSetName)
	} else {
		tableName = fmt.Sprintf(tableNameTemplate, dataSet.DataSetName)
	}

	return fmt.Sprintf(createTableQueryTemplate, tableName, columnsString)
}
