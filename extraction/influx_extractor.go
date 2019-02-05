package extraction

import (
	"fmt"
	"log"

	"github.com/timescale/outflux/utils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start(utils.ErrorBroadcaster) chan idrf.Row
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	config   *config.Config
	producer DataProducer
}

// NewExtractor creates a new instance of InfluxExtractor with the specified config and connection params
func NewExtractor(extractionConfig *config.Config) (InfluxExtractor, error) {
	err := config.ValidateMeasureExtractionConfig(extractionConfig.MeasureExtraction)
	if err != nil {
		return nil, fmt.Errorf("Config not valid: %s", err.Error())
	}

	if extractionConfig.Connection == nil {
		return nil, fmt.Errorf("Connection params can't be nil")
	}

	if extractionConfig.DataSet == nil {
		return nil, fmt.Errorf("DataSet info can not be nil")
	}

	clientUtils := clientutils.NewUtils()
	return &defaultInfluxExtractor{
		config:   extractionConfig,
		producer: NewDataProducerWith(extractionConfig.ExtractorID, clientUtils),
	}, nil
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *defaultInfluxExtractor) Start(errorBroadcaster utils.ErrorBroadcaster) chan idrf.Row {
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
	dataChannel := make(chan idrf.Row, ie.config.MeasureExtraction.DataChannelBufferSize)

	go ie.producer.Fetch(ie.config.Connection, dataChannel, query, errorBroadcaster)

	return dataChannel
}
