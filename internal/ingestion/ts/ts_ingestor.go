package ts

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// TSIngestor implements a TimescaleDB ingestor
type TSIngestor struct {
	Config           *config.IngestorConfig
	DbConn           *pgx.Conn
	IngestionRoutine Routine
	SchemaManager    schemamanagement.SchemaManager
	cachedBundle     *idrf.Bundle
}

// ID returns a string identifying the ingestor instance in logs
func (i *TSIngestor) ID() string {
	return i.Config.IngestorID
}

// Prepare creates or validates the output tables in Timescale
func (i *TSIngestor) Prepare(bundle *idrf.Bundle) error {
	i.cachedBundle = bundle
	return i.SchemaManager.PrepareDataSet(bundle.DataDef, i.Config.SchemaStrategy)
}

// Start consumes a data channel of idrf.Row(s) and inserts them into a TimescaleDB hypertable
func (i *TSIngestor) Start(errChan chan error) error {
	if i.cachedBundle == nil {
		return fmt.Errorf("%s: Start called without calling Prepare first", i.Config.IngestorID)
	}

	dataSet := i.cachedBundle.DataDef
	colNames := extractColumnNames(dataSet.Columns)

	schema, table := dataSet.SchemaAndTable()
	ingestArgs := &ingestDataArgs{
		ingestorID:              i.Config.IngestorID,
		errChan:                 errChan,
		dataChannel:             i.cachedBundle.DataChan,
		rollbackOnExternalError: i.Config.RollbackOnExternalError,
		batchSize:               i.Config.BatchSize,
		dbConn:                  i.DbConn,
		colNames:                colNames,
		tableName:               table,
		schemaName:              schema,
		commitStrategy:          i.Config.CommitStrategy,
	}

	return i.IngestionRoutine.ingest(ingestArgs)
}

func extractColumnNames(columns []*idrf.ColumnInfo) []string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	return columnNames
}
