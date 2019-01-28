package ingestion

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/timescale/outflux/extraction"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/ingestion/schemamanagement"
)

const (
	// example: postgres://test:test@localhost:5432/test?sslmode=disable
	postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s%s"
)

type Ingestor interface {
	Start(extractionInfo *extraction.ExtractionInfo) (chan bool, error)
}

func NewIngestor(config *config.Config) Ingestor {
	return &defaultIngestor{
		config:             config,
		converterGenerator: NewIdrfConverterGenerator(),
		ingestionRoutine:   NewIngestionRoutine(),
		schemaManager:      schemamanagement.NewSchemaManager(),
	}
}

type defaultIngestor struct {
	config             *config.Config
	converterGenerator IdrfConverterGenerator
	ingestionRoutine   Routine
	schemaManager      schemamanagement.SchemaManager
}

func (ing *defaultIngestor) Start(extractionInfo *extraction.ExtractionInfo) (chan bool, error) {
	dataSet := extractionInfo.DataSetSchema
	ackChannel := make(chan bool)
	connStr := buildConnectionString(ing.config)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to target database: %s", err.Error())
	}

	// validate that db is ready to receive data
	managerArgs := &schemamanagement.PrepareArgs{
		Strategy: ing.config.SchemaStrategy,
		Schema:   ing.config.Schema,
		DataSet:  extractionInfo.DataSetSchema,
		DbCon:    db,
	}
	err = ing.schemaManager.Prepare(managerArgs)
	if err != nil {
		return nil, fmt.Errorf("couldn't prepare input db. %s", err.Error())
	}

	transaction, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("couldn't open a transaction. %s", err.Error())
	}

	columnNames := extractColumnNames(dataSet.Columns)
	copyQuery := pq.CopyIn(dataSet.DataSetName, columnNames...)
	statement, err := transaction.Prepare(copyQuery)
	if err != nil {
		return nil, fmt.Errorf("couldn't prepare COPY FROM statement in transaction. %s", err.Error())
	}

	ingestArgs := &ingestDataArgs{
		ackChannel:        ackChannel,
		preparedStatement: statement,
		transaction:       transaction,
		extractionInfo:    extractionInfo,
		converter:         ing.converterGenerator.Generate(extractionInfo.DataSetSchema),
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
