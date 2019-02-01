package extraction

import (
	"fmt"

	"github.com/timescale/outflux/utils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start(utils.ErrorBroadcaster) (chan idrf.Row, error)
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	config   *config.Config
	producer DataProducer
	logger   utils.Logger
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
		logger:   utils.NewLogger(extractionConfig.Quiet),
	}, nil
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *defaultInfluxExtractor) Start(errorBroadcaster utils.ErrorBroadcaster) (chan idrf.Row, error) {
	ie.logger.Log(fmt.Sprintf("Starting extractor '%s' for measure: %s", ie.config.ExtractorID, ie.config.DataSet.DataSetName))
	intChunkSize := int(ie.config.MeasureExtraction.ChunkSize)

	query := influx.Query{
		Command:   buildSelectCommand(ie.config.MeasureExtraction, ie.config.DataSet.Columns),
		Database:  ie.config.MeasureExtraction.Database,
		Chunked:   true,
		ChunkSize: intChunkSize,
	}

	ie.logger.Log(fmt.Sprintf("Extracting data from server '%s', database '%s'", ie.config.Connection.Server, query.Database))
	ie.logger.Log(fmt.Sprintf("Connecting with user '%s'", ie.config.Connection.Username))
	ie.logger.Log(fmt.Sprintf("SELECT query: %s", query.Command))
	ie.logger.Log(fmt.Sprintf("Pulling chunks with size %d", intChunkSize))
	dataChannel := make(chan idrf.Row, ie.config.MeasureExtraction.DataChannelBufferSize)

	go ie.producer.Fetch(ie.config.Connection, dataChannel, query, errorBroadcaster, ie.logger)

	return dataChannel, nil
}
