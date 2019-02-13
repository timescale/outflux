package ingestion

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
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
	Start(errorBroadcaster utils.ErrorBroadcaster) (chan bool, error)
}

// NewIngestor creates a new instance of an Ingestor with a specified config, for a specified
// data set and data channel
func NewIngestor(
	config *IngestorConfig,
	schemaManager schemamanagement.SchemaManager,
	dbConn *sql.DB,
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
	dbConn           *sql.DB
	dataSet          *idrf.DataSetInfo
	dataChannel      chan idrf.Row
}

func (ing *defaultIngestor) Start(errorBroadcaster utils.ErrorBroadcaster) (chan bool, error) {
	id := ing.config.IngestorID
	ackChannel := make(chan bool)

	transaction, err := ing.dbConn.Begin()
	if err != nil {
		err = fmt.Errorf("%s: couldn't open a transaction.\n%v", id, err)
		errorBroadcaster.Broadcast(ing.config.IngestorID, err)
		return nil, err
	}

	columnNames := extractColumnNames(ing.dataSet.Columns)
	copyQuery := pq.CopyIn(ing.dataSet.DataSetName, columnNames...)
	statement, err := transaction.Prepare(copyQuery)
	if err != nil {
		err = fmt.Errorf("%s: couldn't prepare COPY FROM statement in transaction\n%v", id, err)
		errorBroadcaster.Broadcast(ing.config.IngestorID, err)
		return nil, err
	}

	ingestArgs := &ingestDataArgs{
		ingestorID:              ing.config.IngestorID,
		errorBroadcaster:        errorBroadcaster,
		ackChannel:              ackChannel,
		preparedStatement:       statement,
		transaction:             transaction,
		dataChannel:             ing.dataChannel,
		converter:               ing.converter,
		rollbackOnExternalError: ing.config.RollbackOnExternalError,
		batchSize:               ing.config.BatchSize,
		dbConn:                  ing.dbConn,
	}

	go ing.ingestionRoutine.ingestData(ingestArgs)
	return ackChannel, nil
}

func extractColumnNames(columns []*idrf.ColumnInfo) []string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	return columnNames
}
