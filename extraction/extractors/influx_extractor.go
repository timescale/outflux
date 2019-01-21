package extractors

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	influxUtils "github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start() (*ExtractedInfo, error)
}

// GenerateExtractorFn is a type signature of a function that can create instances of InfluxExtractor
type GenerateExtractorFn func(*config.MeasureExtraction, *influxUtils.ConnectionParams) (InfluxExtractor, error)

// ExtractedInfo returned when starting an extractor. Contains the data, error channels and schema
type ExtractedInfo struct {
	dataChannel   chan idrf.Row
	errorChannel  chan error
	dataSetSchema *idrf.DataSetInfo
}
