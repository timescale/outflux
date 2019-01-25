package extraction

import (
	"fmt"

	"github.com/timescale/outflux/utils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start(utils.ErrorBroadcaster) (*ExtractionInfo, error)
}

// ExtractedInfo returned when starting an extractor. Contains the data, error channels and schema
type ExtractedInfo struct {
	DataChannel   chan idrf.Row
	DataSetSchema *idrf.DataSetInfo
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	ID             string
	config         *config.MeasureExtraction
	connection     *clientutils.ConnectionParams
	schemaExplorer schemadiscovery.SchemaExplorer
	influxUtils    clientutils.ClientUtils
	producer       DataProducer
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

	clientUtils := clientutils.NewUtils()
	return &defaultInfluxExtractor{
		config:         extractionConfig.MeasureExtraction,
		connection:     extractionConfig.Connection,
		ID:             extractionConfig.ExtractorID,
		schemaExplorer: schemadiscovery.NewSchemaExplorerWithUtils(clientUtils),
		influxUtils:    clientUtils,
		producer:       NewDataProducerWith(extractionConfig.ExtractorID, clientUtils),
	}, nil
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *defaultInfluxExtractor) Start(errorBroadcaster utils.ErrorBroadcaster) (*ExtractionInfo, error) {
	dataSetInfo, err := ie.schemaExplorer.InfluxMeasurementSchema(ie.connection, ie.config.Database, ie.config.Measure)
	if err != nil {
		return nil, fmt.Errorf("couldn't discover influxdb schema\n%v", err)
	}

	intChunkSize, err := safeCastChunkSize(ie.config.ChunkSize)
	if err != nil {
		return nil, err
	}

	query := influx.Query{
		Command:   buildSelectCommand(ie.config, dataSetInfo.Columns),
		Database:  ie.config.Database,
		Chunked:   true,
		ChunkSize: intChunkSize,
	}

	dataChannel := make(chan idrf.Row, ie.config.DataChannelBufferSize)

	go ie.producer.Fetch(ie.connection, dataChannel, query, errorBroadcaster)

	return &ExtractedInfo{
		DataSetSchema: dataSetInfo,
		DataChannel:   dataChannel,
	}, nil
}

func safeCastChunkSize(num uint) (int, error) {
	numInt := int(num)
	if numInt < 0 || uint(numInt) != num {
		return -1, fmt.Errorf("chunk size could not be safely expressed as a signed int, it's too large")
	}

	return numInt, nil
}
