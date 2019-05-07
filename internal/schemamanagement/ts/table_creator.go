package ts

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/idrf"
)

const (
	createTableQueryTemplate                = "CREATE TABLE \"%s\"(%s)"
	createTableWithSchemaQueryTemplate      = "CREATE TABLE \"%s\".\"%s\"(%s)"
	columnDefTemplate                       = "\"%s\" %s"
	createHypertableQueryTemplate           = "SELECT create_hypertable('\"%s\"', '%s');"
	createHypertableWithSchemaQueryTemplate = "SELECT create_hypertable('\"%s\".\"%s\"', '%s');"
	createTimescaleExtensionQuery           = "CREATE EXTENSION IF NOT EXISTS timescaledb"
	metadataKey                             = "outflux_last_usage"
	getMetadataTemplate                     = "SELECT EXISTS (SELECT 1 FROM %s.%s WHERE key = $1) "
	insertMetadataTemplate                  = "INSERT INTO %s.%s VALUES($1, $2)"
	updateMetadataTemplate                  = "UPDATE %s.%s SET value=$1 WHERE key=$2"
)

type tableCreator interface {
	CreateTable(dbConn *pgx.Conn, info *idrf.DataSet) error
	CreateHypertable(dbConn *pgx.Conn, info *idrf.DataSet) error
	CreateTimescaleExtension(dbConn *pgx.Conn) error
	UpdateMetadata(dbConn *pgx.Conn, metadataTableName string) error
}

func newTableCreator() tableCreator {
	return &defaultTableCreator{}
}

type defaultTableCreator struct{}

func (d *defaultTableCreator) CreateTable(dbConn *pgx.Conn, info *idrf.DataSet) error {
	query := dataSetToSQLTableDef(info)
	log.Printf("Creating table with:\n %s", query)

	_, err := dbConn.Exec(query)
	if err != nil {
		return err
	}

	log.Printf("Preparing TimescaleDB extension:\n%s", createTimescaleExtensionQuery)
	_, err = dbConn.Exec(createTimescaleExtensionQuery)
	if err != nil {
		return err
	}

	schema, table := info.SchemaAndTable()
	var hypertableQuery string
	if schema != "" {
		hypertableQuery = fmt.Sprintf(createHypertableWithSchemaQueryTemplate, schema, table, info.TimeColumn)
	} else {
		hypertableQuery = fmt.Sprintf(createHypertableQueryTemplate, info.DataSetName, info.TimeColumn)
	}

	log.Printf("Creating hypertable with: %s", hypertableQuery)
	_, err = dbConn.Exec(hypertableQuery)
	if err != nil {
		return err
	}

	return nil
}

func (d *defaultTableCreator) CreateHypertable(dbConn *pgx.Conn, info *idrf.DataSet) error {
	hypertableQuery := fmt.Sprintf(createHypertableQueryTemplate, info.DataSetName, info.TimeColumn)
	log.Printf("Creating hypertable with: %s", hypertableQuery)
	_, err := dbConn.Exec(hypertableQuery)
	return err
}

func (d *defaultTableCreator) CreateTimescaleExtension(dbConn *pgx.Conn) error {
	log.Printf("Preparing TimescaleDB extension:\n%s", createTimescaleExtensionQuery)
	_, err := dbConn.Exec(createTimescaleExtensionQuery)
	return err
}

func (d *defaultTableCreator) UpdateMetadata(dbConn *pgx.Conn, metadataTableName string) error {
	log.Printf("Updating Timescale metadata")
	metadataQuery := fmt.Sprintf(getMetadataTemplate, timescaleCatalogSchema, metadataTableName)
	rows, err := dbConn.Query(metadataQuery, metadataKey)
	if err != nil {
		return fmt.Errorf("Could not check if Outflux metadata already exists. %v", err)
	}
	exists := false
	if !rows.Next() {
		rows.Close()
		return fmt.Errorf("Could not check if Outflux metadata already exists. %v", err)
	}
	err = rows.Scan(&exists)
	if err != nil {
		rows.Close()
		return fmt.Errorf("Could not check if Outflux installation metadata already exists. %v", err)
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

func dataSetToSQLTableDef(dataSet *idrf.DataSet) string {
	columnDefinitions := make([]string, len(dataSet.Columns))
	for i, column := range dataSet.Columns {
		dataType := idrfToPgType(column.DataType)
		columnDefinitions[i] = fmt.Sprintf(columnDefTemplate, column.Name, dataType)
	}

	columnsString := strings.Join(columnDefinitions, ", ")

	schema, table := dataSet.SchemaAndTable()
	if schema != "" {
		return fmt.Sprintf(createTableWithSchemaQueryTemplate, schema, table, columnsString)
	}

	return fmt.Sprintf(createTableQueryTemplate, table, columnsString)
}
