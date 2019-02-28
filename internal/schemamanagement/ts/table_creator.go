package ts

import (
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/idrf"
)

const (
	createTableQueryTemplate      = "CREATE TABLE %s(%s)"
	columnDefTemplate             = "%s %s"
	createHypertableQueryTemplate = "SELECT create_hypertable('%s', '%s');"
	createTimescaleExtensionQuery = "CREATE EXTENSION IF NOT EXISTS timescaledb"
)

type tableCreator interface {
	CreateTable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error
	CreateHypertable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error
	CreateTimescaleExtension(dbConn *pgx.Conn) error
}

func newTableCreator() tableCreator {
	return &defaultTableCreator{}
}

type defaultTableCreator struct{}

func (d *defaultTableCreator) CreateTable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error {
	tableName := info.DataSetName
	query := dataSetToSQLTableDef(tableName, info)
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

	hypertableQuery := fmt.Sprintf(createHypertableQueryTemplate, tableName, info.TimeColumn)
	log.Printf("Creating hypertable with: %s", hypertableQuery)
	_, err = dbConn.Exec(hypertableQuery)
	if err != nil {
		return err
	}

	return nil
}

func (d *defaultTableCreator) CreateHypertable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error {
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

func dataSetToSQLTableDef(tableName string, dataSet *idrf.DataSetInfo) string {
	columnDefinitions := make([]string, len(dataSet.Columns))
	for i, column := range dataSet.Columns {
		dataType := idrfToPgType(column.DataType)
		columnDefinitions[i] = fmt.Sprintf(columnDefTemplate, column.Name, dataType)
	}

	columnsString := strings.Join(columnDefinitions, ", ")

	return fmt.Sprintf(createTableQueryTemplate, tableName, columnsString)
}
