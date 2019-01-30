package extraction

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start() (*ExtractionInfo, error)
}

// ExtractionInfo returned when starting an extractor. Contains the data, error channels and schema
type ExtractionInfo struct {
	DataChannel   chan idrf.Row
	ErrorChannel  chan error
	DataSetSchema *idrf.DataSetInfo
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	config         *config.MeasureExtraction
	connection     *clientutils.ConnectionParams
	schemaExplorer schemadiscovery.SchemaExplorer
	influxUtils    clientutils.ClientUtils
	producer       DataProducer
}

// NewExtractor creates a new instance of InfluxExtractor with the specified config and connection params
func NewExtractor(measureConfig *config.MeasureExtraction, connection *clientutils.ConnectionParams) (InfluxExtractor, error) {
	err := config.ValidateMeasureExtractionConfig(measureConfig)
	if err != nil {
		return nil, fmt.Errorf("Config not valid: %s", err.Error())
	}

	if connection == nil {
		return nil, fmt.Errorf("Connection params can't be nil")
	}

	clientUtils := clientutils.NewUtils()
	return &defaultInfluxExtractor{
		config:         measureConfig,
		connection:     connection,
		schemaExplorer: schemadiscovery.NewSchemaExplorerWithUtils(clientUtils),
		influxUtils:    clientUtils,
		producer:       NewDataProducerWith(clientUtils),
	}, nil
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *defaultInfluxExtractor) Start() (*ExtractionInfo, error) {
	dataSetInfo, err := ie.schemaExplorer.InfluxMeasurementSchema(ie.connection, ie.config.Database, ie.config.Measure)
	if err != nil {
		return nil, err
	}

	intChunkSize := int(ie.config.ChunkSize)

	query := influx.Query{
		Command:   buildSelectCommand(ie.config, dataSetInfo.Columns),
		Database:  ie.config.Database,
		Chunked:   true,
		ChunkSize: intChunkSize,
	}

	dataChannel := make(chan idrf.Row, ie.config.DataChannelBufferSize)
	errorChannel := make(chan error)

	go ie.producer.Fetch(ie.connection, dataChannel, errorChannel, query)

	return &ExtractionInfo{
		DataSetSchema: dataSetInfo,
		DataChannel:   dataChannel,
		ErrorChannel:  errorChannel,
	}, nil
}
