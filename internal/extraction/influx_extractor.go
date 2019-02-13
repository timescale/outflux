package extraction

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/utils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start(utils.ErrorBroadcaster)
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	config   *config.Config
	producer DataProducer
}

// NewExtractor creates a new instance of InfluxExtractor with the specified config and connection params
func NewExtractor(extractionConfig *config.Config, connectionService connections.InfluxConnectionService) (InfluxExtractor, error) {
	err := config.ValidateMeasureExtractionConfig(extractionConfig.MeasureExtraction)
	if err != nil {
		return nil, fmt.Errorf("measure extraction config is not valid: %s", err.Error())
	}

	if extractionConfig.Connection == nil {
		return nil, fmt.Errorf("connection params can't be nil")
	}

	if extractionConfig.DataSet == nil {
		return nil, fmt.Errorf("DataSet info can not be nil")
	}

	return &defaultInfluxExtractor{
		config:   extractionConfig,
		producer: NewDataProducer(extractionConfig.ExtractorID, connectionService),
	}, nil
}

// Start builds a select query to extract a measurement from an Influx DB specified in the config
// and enques the extracted rows in the data channel also provided in the config
func (ie *defaultInfluxExtractor) Start(errorBroadcaster utils.ErrorBroadcaster) {
	id := ie.config.ExtractorID
	log.Printf("Starting extractor '%s' for measure: %s\n", id, ie.config.DataSet.DataSetName)
	intChunkSize := int(ie.config.MeasureExtraction.ChunkSize)

	query := influx.Query{
		Command:   buildSelectCommand(ie.config.MeasureExtraction, ie.config.DataSet.Columns),
		Database:  ie.config.MeasureExtraction.Database,
		Chunked:   true,
		ChunkSize: intChunkSize,
	}

	log.Printf("%s: Extracting data from server '%s', database '%s'\n", id, ie.config.Connection.Server, query.Database)
	log.Printf("%s: Connecting with user '%s'\n", id, ie.config.Connection.Username)
	log.Printf("%s: %s\n", id, query.Command)
	log.Printf("%s:Pulling chunks with size %d\n", id, intChunkSize)

	go ie.producer.Fetch(ie.config.Connection, ie.config.DataChannel, query, errorBroadcaster)
}
