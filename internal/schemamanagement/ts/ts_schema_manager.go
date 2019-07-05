package ts

import (
	"fmt"
	"log"

	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// TSSchemaManager implements the schemamanagement.SchemaManager interface for TimescaleDB
type TSSchemaManager struct {
	explorer schemaExplorer
	creator  tableCreator
	dropper  tableDropper
	dbConn   *pgx.Conn
}

// NewTSSchemaManager creates a new TimeScale Schema Manager
func NewTSSchemaManager(dbConn *pgx.Conn) *TSSchemaManager {
	return &TSSchemaManager{
		dbConn:   dbConn,
		explorer: newSchemaExplorer(),
		creator:  newTableCreator(),
		dropper:  newTableDropper(),
	}
}

// DiscoverDataSets not implemented
func (sm *TSSchemaManager) DiscoverDataSets() ([]string, error) {
	panic(fmt.Errorf("not implemented"))
}

// FetchDataSet not implemented
func (sm *TSSchemaManager) FetchDataSet(dataSetIdentifier string) (*idrf.DataSet, error) {
	panic(fmt.Errorf("not implemented"))
}

// PrepareDataSet prepares a table in TimeScale compatible with the provided dataSet
func (sm *TSSchemaManager) PrepareDataSet(dataSet *idrf.DataSet, strategy schemaconfig.SchemaStrategy) error {
	log.Printf("Selected Schema Strategy: %s", strategy.String())
	schema, table := dataSet.SchemaAndTable()
	tableExists, err := sm.explorer.tableExists(sm.dbConn, schema, table)
	if err != nil {
		return fmt.Errorf("could not prepare data set '%s'. Could not check if table exists. \n%v", dataSet.DataSetName, err)
	}

	var preparationError error
	switch strategy {
	case schemaconfig.DropAndCreate:
		preparationError = sm.prepareWithDropStrategy(dataSet, strategy, tableExists)
	case schemaconfig.DropCascadeAndCreate:
		preparationError = sm.prepareWithDropStrategy(dataSet, strategy, tableExists)
	case schemaconfig.CreateIfMissing:
		preparationError = sm.prepareWithCreateIfMissing(dataSet, tableExists)
	case schemaconfig.ValidateOnly:
		preparationError = sm.validateOnly(dataSet, tableExists)
	default:
		panic("unexpected type")
	}

	if preparationError != nil {
		return preparationError
	}

	sm.updateMetadata()
	return nil
}

func (sm *TSSchemaManager) validateOnly(dataSet *idrf.DataSet, tableExists bool) error {
	if !tableExists {
		return fmt.Errorf("validate only strategy selected, but '%s' doesn't exist", dataSet.DataSetName)
	}

	log.Printf("Table %s exists. Proceeding only with validation", dataSet.DataSetName)
	if err := sm.validateColumns(dataSet); err != nil {
		return err
	}

	timescaleExists, err := sm.explorer.timescaleExists(sm.dbConn)
	if err != nil {
		return fmt.Errorf("could not check if TimescaleDB is installed\n%v", err)
	}
	if !timescaleExists {
		return fmt.Errorf("timescaledb extension not installed in database")
	}

	schema, table := dataSet.SchemaAndTable()
	isHypertable, err := sm.explorer.isHypertable(sm.dbConn, schema, table)
	if err != nil {
		return fmt.Errorf("could not check if table %s is hypertable", dataSet.DataSetName)
	}
	if !isHypertable {
		return fmt.Errorf("existing table %s is not a hypertable", dataSet.DataSetName)
	}

	return sm.validatePartitioning(dataSet)
}
func (sm *TSSchemaManager) prepareWithDropStrategy(dataSet *idrf.DataSet, strategy schemaconfig.SchemaStrategy, tableExists bool) error {
	if tableExists {
		log.Printf("Table %s exists, dropping it", dataSet.DataSetName)
		cascade := strategy == schemaconfig.DropCascadeAndCreate
		err := sm.dropper.Drop(sm.dbConn, dataSet.DataSetName, cascade)
		if err != nil {
			return fmt.Errorf("selected schema strategy wanted to drop the existing table, but: %s", err.Error())
		}
	}

	log.Printf("Table %s ready to be created", dataSet.DataSetName)
	return sm.creator.CreateTable(sm.dbConn, dataSet)
}

func (sm *TSSchemaManager) prepareWithCreateIfMissing(dataSet *idrf.DataSet, tableExists bool) error {
	if !tableExists {
		log.Printf("CreateIfMissing strategy: Table %s does not exist. Creating", dataSet.DataSetName)
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

	schema, table := dataSet.SchemaAndTable()
	isHypertable, err := sm.explorer.isHypertable(sm.dbConn, schema, table)
	if err != nil {
		return fmt.Errorf("could not check if table %s is hypertable", dataSet.DataSetName)
	}

	if !isHypertable {
		err := sm.creator.CreateHypertable(sm.dbConn, dataSet)
		return err
	}

	return sm.validatePartitioning(dataSet)

}

func (sm *TSSchemaManager) validateColumns(dataSet *idrf.DataSet) error {
	schema, table := dataSet.SchemaAndTable()
	existingTableColumns, err := sm.explorer.fetchTableColumns(sm.dbConn, schema, table)
	if err != nil {
		return fmt.Errorf("could not retriеve column information for table %s", dataSet.DataSetName)
	}

	err = isExistingTableCompatible(existingTableColumns, dataSet.Columns, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("existing table in target db is not compatible with required. %v", err)
	}

	return nil
}

func (sm *TSSchemaManager) validatePartitioning(dataSet *idrf.DataSet) error {
	schema, table := dataSet.SchemaAndTable()
	isPartitionedProperly, err := sm.explorer.isTimePartitionedBy(sm.dbConn, schema, table, dataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("could not check if existing hypertable '%s' is partitioned properly\n%v", dataSet.DataSetName, err)
	}

	if !isPartitionedProperly {
		return fmt.Errorf("existing hypertable '%s' is not partitioned by timestamp column: %s", dataSet.DataSetName, dataSet.TimeColumn)
	}

	log.Printf("existing hypertable '%s' is partitioned properly", dataSet.DataSetName)
	return nil
}

func (sm *TSSchemaManager) updateMetadata() {
	metadataTableName, err := sm.explorer.metadataTableName(sm.dbConn)
	if err != nil {
		log.Println("could not check for the existence of the timescale metadata table")
		log.Println("reason: " + err.Error())
		return
	}

	if metadataTableName == "" {
		log.Println("Installation metadata table doesn't exist in this TimescaleDB version")
		return
	}

	err = sm.creator.UpdateMetadata(sm.dbConn, metadataTableName)
	if err != nil {
		log.Println("could not update TimescaleDB metadata")
		log.Println("reason:" + err.Error())
	}
}
