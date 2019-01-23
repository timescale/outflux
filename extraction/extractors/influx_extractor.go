package extractors

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s FROM \"%s\" WHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s FROM \"%s\""
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start() (*ExtractedInfo, error)
}

// ExtractedInfo returned when starting an extractor. Contains the data, error channels and schema
type ExtractedInfo struct {
	dataChannel   chan idrf.Row
	errorChannel  chan error
	dataSetSchema *idrf.DataSetInfo
}

// defaultInfluxExtractor is an implementation of the extractor interface.
type defaultInfluxExtractor struct {
	config         *config.MeasureExtraction
	connection     *clientutils.ConnectionParams
	schemaExplorer schemadiscovery.SchemaExplorer
	influxUtils    clientutils.ClientUtils
	producer       DataProducer
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *defaultInfluxExtractor) Start() (*ExtractedInfo, error) {
	dataSetInfo, err := ie.schemaExplorer.InfluxMeasurementSchema(ie.connection, ie.config.Database, ie.config.Measure)
	if err != nil {
		return nil, err
	}

	query := influx.Query{
		Command:   buildSelectCommand(ie.config, dataSetInfo.Columns),
		Database:  ie.config.Database,
		Chunked:   true,
		ChunkSize: ie.config.ChunkSize,
	}

	dataChannel := make(chan idrf.Row)
	errorChannel := make(chan error)

	go ie.producer.Fetch(ie.connection, dataChannel, errorChannel, query)

	return &ExtractedInfo{
		dataSetSchema: dataSetInfo,
		dataChannel:   dataChannel,
		errorChannel:  errorChannel,
	}, nil
}
