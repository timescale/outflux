package ts

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"log"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type tsSchemaManager struct {
	explorer schemaExplorer
	creator  tableCreator
	dropper  tableDropper
	dbConn   *sql.DB
	pgxConn  *pgx.Conn
}

// NewTSSchemaManager creates a new TimeScale Schema Manager
func NewTSSchemaManager(dbConn *sql.DB, dbConn2 *pgx.Conn) schemamanagement.SchemaManager {
	return &tsSchemaManager{
		dbConn:   dbConn,
		pgxConn:  dbConn2,
		explorer: newSchemaExplorer(),
		creator:  newTableCreator(),
		dropper:  newTableDropper(),
	}
}
func (sm *tsSchemaManager) DiscoverDataSets() ([]*idrf.DataSetInfo, error) {
	panic(fmt.Errorf("not implemented"))
}

func (sm *tsSchemaManager) FetchDataSet(schema, name string) (*idrf.DataSetInfo, error) {
	panic(fmt.Errorf("not implemented"))
}

func (sm *tsSchemaManager) PrepareDataSet(dataSet *idrf.DataSetInfo, strategy schemamanagement.SchemaStrategy) error {
	log.Printf("Selected Schema Strategy: %s", strategy.String())
	tableExists, err := sm.explorer.tableExists(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not prepare data set '%s'. Could not check if table exists. \n%v", dataSet.DataSetName, err)
	}

	strategyRequiresDrop := strategy == schemamanagement.DropAndCreate || strategy == schemamanagement.DropCascadeAndCreate
	if strategyRequiresDrop {
		return sm.prepareWithDropStrategy(dataSet, strategy, tableExists)
	} else if strategy == schemamanagement.CreateIfMissing && !tableExists {
		log.Printf("CreateIfMissing strategy: Table %s does not exist. Creating", dataSet.DataSetName)
		return sm.creator.Create(sm.pgxConn, dataSet)
	} else if strategy != schemamanagement.CreateIfMissing && strategy != schemamanagement.ValidateOnly {
		return fmt.Errorf("preparation step for strategy %v not implemented", strategy)
	} else if strategy == schemamanagement.ValidateOnly && !tableExists {
		return fmt.Errorf("validate only strategy selected, but table is not created in db")
	}

	log.Printf("Table %s.%s exists. Proceding only with validation", dataSet.DataSetSchema, dataSet.DataSetName)
	existingTableColumns, err := sm.explorer.fetchTableColumns(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not retreive column information for table %s", dataSet.DataSetName)
	}

	err = isExistingTableCompatible(existingTableColumns, dataSet.Columns, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("existing table in target db is not compatible with required. %v", err)
	}

	timescaleExists, err := sm.explorer.timescaleExists(sm.dbConn)
	if err != nil {
		return fmt.Errorf("could not check if TimescaleDB is installed")
	}

	if !timescaleExists {
		return fmt.Errorf("timescaledb extension not installed in database")
	}

	isHypertable, err := sm.explorer.isHypertable(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not check if table %s is hypertable", dataSet.DataSetName)
	}

	if !isHypertable {
		return fmt.Errorf("existing table %s is not a hypertable", dataSet.DataSetName)
	}

	isPartitionedProperly, err := sm.explorer.isTimePartitionedBy(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("could not check if existing hypertable '%s' is partitioned properly\n%v", dataSet.DataSetName, err)
	}

	if !isPartitionedProperly {
		return fmt.Errorf("existing hypertable '%s' is not partitioned by timestamp column: %s", dataSet.DataSetName, dataSet.TimeColumn)
	}

	log.Printf("Table is compatible, TimescaleDB is installed and hyptertable is properly set up for partitioning")
	return nil
}

func (sm *tsSchemaManager) prepareWithDropStrategy(dataSet *idrf.DataSetInfo, strategy schemamanagement.SchemaStrategy, tableExists bool) error {
	if tableExists {
		log.Printf("Table %s.%s exists, dropping it", dataSet.DataSetSchema, dataSet.DataSetName)
		cascade := strategy == schemamanagement.DropCascadeAndCreate
		err := sm.dropper.Drop(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName, cascade)
		if err != nil {
			return fmt.Errorf("selected schema strategy wanted to drop the existing table, but: %s", err.Error())
		}
	}

	log.Printf("Table %s.%s ready to be created", dataSet.DataSetSchema, dataSet.DataSetName)
	return sm.creator.Create(sm.pgxConn, dataSet)
}
