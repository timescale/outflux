package extractors

import (
	"fmt"

	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	utils "github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractorImpl is an implementation of the extractor interface.
type influxExtractorImpl struct {
	config     *config.MeasureExtraction
	connection *utils.ConnectionParams
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *influxExtractorImpl) Start(rowChannel chan idrf.Row) (*idrf.DataSetInfo, error) {
	return nil, nil
}

// Stop the extractor from fetching more data
func (ie *influxExtractorImpl) Stop() error {
	return nil
}

// NewInfluxExtractor creates an implementation of the InfluxExtractor interface while checking the arguments
func NewInfluxExtractor(config *config.MeasureExtraction, connection *utils.ConnectionParams) (InfluxExtractor, error) {
	if config == nil || connection == nil {
		return nil, fmt.Errorf("nil not allowed for config or connection")
	}

	return &influxExtractorImpl{config: config, connection: connection}, nil
}
