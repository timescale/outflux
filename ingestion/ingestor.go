package ingestion

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/timescale/outflux/utils"

	"github.com/lib/pq"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/ingestion/schemamanagement"
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
func NewIngestor(config *config.Config, dataSet *idrf.DataSetInfo, dataChannel chan idrf.Row) Ingestor {
	return &defaultIngestor{
		config:           config,
		converter:        newIdrfConverter(dataSet),
		dataChannel:      dataChannel,
		ingestionRoutine: NewIngestionRoutine(),
		schemaManager:    schemamanagement.NewSchemaManager(),
		dataSet:          dataSet,
	}
}

type defaultIngestor struct {
	config           *config.Config
	converter        IdrfConverter
	ingestionRoutine Routine
	schemaManager    schemamanagement.SchemaManager
	dataSet          *idrf.DataSetInfo
	dataChannel      chan idrf.Row
}

func (ing *defaultIngestor) Start(errorBroadcaster utils.ErrorBroadcaster) (chan bool, error) {
	id := ing.config.IngestorID
	ackChannel := make(chan bool)
	connStr := buildConnectionString(ing.config)
	log.Printf("%s: Will connect to output database with: %s", id, connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		err = fmt.Errorf("couldn't connect to target database: %s", err.Error())
		errorBroadcaster.Broadcast(ing.config.IngestorID, err)
		return nil, err
	}

	// validate that db is ready to receive data
	managerArgs := &schemamanagement.PrepareArgs{
		Strategy: ing.config.SchemaStrategy,
		Schema:   ing.config.Schema,
		DataSet:  ing.dataSet,
		DbCon:    db,
	}
	err = ing.schemaManager.Prepare(managerArgs)
	if err != nil {
		err = fmt.Errorf("couldn't prepare input db. %s", err.Error())
		errorBroadcaster.Broadcast(ing.config.IngestorID, err)
		return nil, err
	}

	transaction, err := db.Begin()
	if err != nil {
		err = fmt.Errorf("couldn't open a transaction. %s", err.Error())
		errorBroadcaster.Broadcast(ing.config.IngestorID, err)
		return nil, err
	}

	columnNames := extractColumnNames(ing.dataSet.Columns)
	copyQuery := pq.CopyIn(ing.dataSet.DataSetName, columnNames...)
	statement, err := transaction.Prepare(copyQuery)
	if err != nil {
		err = fmt.Errorf("couldn't prepare COPY FROM statement in transaction. %s", err.Error())
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

func buildConnectionString(config *config.Config) string {
	additionalParams := connectionParamsToString(config.AdditionalConnParams)

	//postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s?%s"
	return fmt.Sprintf(
		postgresConnectionStringTemplate,
		config.Username, config.Password, config.Server, config.Database, additionalParams)
}

func connectionParamsToString(params map[string]string) string {
	if params == nil {
		return ""
	}

	singleParams := make([]string, len(params))
	current := 0
	for key, value := range params {
		singleParams[current] = fmt.Sprintf("%s=%s", key, value)
		current++
	}

	return "?" + strings.Join(singleParams, "&")
}
