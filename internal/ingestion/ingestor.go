package ingestion

import (
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/utils"
)

const (
	// example: postgres://test:test@localhost:5432/test?sslmode=disable
	postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s%s"
)

// Ingestor takes a data channel of idrf rows and inserts them in a target database
type Ingestor interface {
	Start(errorBroadcaster utils.ErrorBroadcaster) chan bool
}

// NewIngestor creates a new instance of an Ingestor with a specified config, for a specified
// data set and data channel
func NewIngestor(
	config *IngestorConfig,
	schemaManager schemamanagement.SchemaManager,
	dbConn *pgx.Conn,
	dataSet *idrf.DataSetInfo,
	dataChannel chan idrf.Row) Ingestor {
	return &defaultIngestor{
		config:           config,
		converter:        newIdrfConverter(dataSet),
		dataChannel:      dataChannel,
		ingestionRoutine: NewIngestionRoutine(),
		dbConn:           dbConn,
		schemaManager:    schemaManager,
		dataSet:          dataSet,
	}
}

type defaultIngestor struct {
	config           *IngestorConfig
	converter        IdrfConverter
	ingestionRoutine Routine
	schemaManager    schemamanagement.SchemaManager
	dbConn           *pgx.Conn
	dataSet          *idrf.DataSetInfo
	dataChannel      chan idrf.Row
}

func (ing *defaultIngestor) Start(errorBroadcaster utils.ErrorBroadcaster) chan bool {
	ackChannel := make(chan bool)

	colNames := extractColumnNames(ing.dataSet.Columns)

	ingestArgs := &ingestDataArgs{
		ingestorID:              ing.config.IngestorID,
		errorBroadcaster:        errorBroadcaster,
		ackChannel:              ackChannel,
		dataChannel:             ing.dataChannel,
		converter:               ing.converter,
		rollbackOnExternalError: ing.config.RollbackOnExternalError,
		batchSize:               ing.config.BatchSize,
		dbConn:                  ing.dbConn,
		colNames:                colNames,
		tableName:               ing.dataSet.DataSetName,
		schemaName:              ing.dataSet.DataSetSchema,
		commitStrategy:          ing.config.CommitStrategy,
	}

	go ing.ingestionRoutine.ingestData(ingestArgs)
	return ackChannel
}

func extractColumnNames(columns []*idrf.ColumnInfo) []string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	return columnNames
}
