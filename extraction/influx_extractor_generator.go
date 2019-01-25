package extraction

import (
	"fmt"

	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// InfluxExtractorGenerator defines a method that returns an instance of an influx db data extractor
type InfluxExtractorGenerator interface {
	Generate(config *config.MeasureExtraction, connection *clientutils.ConnectionParams) (InfluxExtractor, error)
}

type defaultGenerator struct{}

// Generate creates an implementation of the InfluxExtractor interface while checking the arguments
func (dg *defaultGenerator) Generate(
	config *config.MeasureExtraction, connection *clientutils.ConnectionParams,
) (InfluxExtractor, error) {
	if config == nil || connection == nil {
		return nil, fmt.Errorf("nil not allowed for config or connection")
	}

	clientUtils := clientutils.NewUtils()
	defaultProducer := NewDataProducerWith(clientUtils)

	return &defaultInfluxExtractor{
		config:         config,
		connection:     connection,
		schemaExplorer: schemadiscovery.NewSchemaExplorer(),
		producer:       defaultProducer,
		influxUtils:    clientUtils,
	}, nil
}

// NewInfluxExtractorGenerator creates a new instance of a extractor generator
func NewInfluxExtractorGenerator() InfluxExtractorGenerator {
	return &defaultGenerator{}
}
