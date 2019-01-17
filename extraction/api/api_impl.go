package api

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
	influxUtils "github.com/timescale/outflux/schemadiscovery/clientutils"
)

// NewExtractorFn defines the signature of a function that generates an InfluxExtractor,
// from given measure extraction config and connection params
type NewExtractorFn func(*config.MeasureExtraction, *influxUtils.ConnectionParams) (extractors.InfluxExtractor, error)

type apiImpl struct {
	extractorGenerator NewExtractorFn
}

func (api apiImpl) CreateExtractors(config *config.ExtractorConfig) ([]extractors.InfluxExtractor, error) {
	connectionParams := config.Connection
	extractors := make([]extractors.InfluxExtractor, len(config.Measures))
	for index, measureConfig := range config.Measures {
		var err error
		extractors[index], err = api.extractorGenerator(measureConfig, connectionParams)
		if err != nil {
			return nil, err
		}

	}

	return extractors, nil
}

// NewAPI returns an implementation of the extraction api
func NewAPI() API {
	return apiImpl{
		extractorGenerator: extractors.NewInfluxExtractor,
	}
}
