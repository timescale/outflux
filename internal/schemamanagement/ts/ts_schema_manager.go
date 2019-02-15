package ts

import (
	"fmt"
	"log"

	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type tsSchemaManager struct {
	explorer schemaExplorer
	creator  tableCreator
	dropper  tableDropper
	dbConn   *pgx.Conn
}

// NewTSSchemaManager creates a new TimeScale Schema Manager
func NewTSSchemaManager(dbConn *pgx.Conn) schemamanagement.SchemaManager {
	return &tsSchemaManager{
		dbConn:   dbConn,
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

	switch strategy {
	case schemamanagement.DropAndCreate:
		return sm.prepareWithDropStrategy(dataSet, strategy, tableExists)
	case schemamanagement.DropCascadeAndCreate:
		return sm.prepareWithDropStrategy(dataSet, strategy, tableExists)
	case schemamanagement.CreateIfMissing:
		return sm.prepareWithCreateIfMissing(dataSet, tableExists)
	case schemamanagement.ValidateOnly:
		return sm.validateOnly(dataSet, tableExists)
	default:
		panic("unexpected type")
	}
}

func (sm *tsSchemaManager) validateOnly(dataSet *idrf.DataSetInfo, tableExists bool) error {
	if !tableExists {
		return fmt.Errorf("validate only strategy selected, but '%s' doesn't exist", dataSet.FullName())
	}

	log.Printf("Table %s.%s exists. Proceding only with validation", dataSet.DataSetSchema, dataSet.DataSetName)
	if err := sm.validateColumns(dataSet); err != nil {
		return err
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

	return sm.validatePartitioning(dataSet)
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
	return sm.creator.CreateTable(sm.dbConn, dataSet)
}

func (sm *tsSchemaManager) prepareWithCreateIfMissing(dataSet *idrf.DataSetInfo, tableExists bool) error {
	if !tableExists {
		log.Printf("CreateIfMissing strategy: Table %s does not exist. Creating", dataSet.FullName())
		return sm.creator.CreateTable(sm.dbConn, dataSet)
	}

	if err := sm.validateColumns(dataSet); err != nil {
		return err
	}

	timescaleExists, err := sm.explorer.timescaleExists(sm.dbConn)
	if err != nil {
		return fmt.Errorf("could not check if TimescaleDB is installed")
	}

	if !timescaleExists {
		err := sm.creator.CreateTimescaleExtension(sm.dbConn)
		if err != nil {
			return err
		}
	}

	isHypertable, err := sm.explorer.isHypertable(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not check if table %s is hypertable", dataSet.DataSetName)
	}

	if !isHypertable {
		err := sm.creator.CreateHypertable(sm.dbConn, dataSet)
		return err
	}

	return sm.validatePartitioning(dataSet)

}

func (sm *tsSchemaManager) validateColumns(dataSet *idrf.DataSetInfo) error {
	existingTableColumns, err := sm.explorer.fetchTableColumns(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not retreive column information for table %s", dataSet.FullName())
	}

	err = isExistingTableCompatible(existingTableColumns, dataSet.Columns, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("existing table in target db is not compatible with required. %v", err)
	}

	return nil
}

func (sm *tsSchemaManager) validatePartitioning(dataSet *idrf.DataSetInfo) error {
	isPartitionedProperly, err := sm.explorer.isTimePartitionedBy(sm.dbConn, dataSet.DataSetSchema, dataSet.DataSetName, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("could not check if existing hypertable '%s' is partitioned properly\n%v", dataSet.DataSetName, err)
	}

	if !isPartitionedProperly {
		return fmt.Errorf("existing hypertable '%s' is not partitioned by timestamp column: %s", dataSet.FullName(), dataSet.TimeColumn)
	}

	log.Printf("existing hypertable '%s' is partitioned properly", dataSet.FullName())
	return nil
}
