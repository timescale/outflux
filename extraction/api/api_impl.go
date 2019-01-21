package api

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
)

type defaultExtractorGenerator struct {
	generate extractors.GenerateExtractorFn
}

func (api *defaultExtractorGenerator) CreateExtractors(config *config.ExtractorConfig) ([]extractors.InfluxExtractor, error) {
	connectionParams := config.Connection
	extractors := make([]extractors.InfluxExtractor, len(config.Measures))
	for index, measureConfig := range config.Measures {
		var err error
		extractors[index], err = api.generate(measureConfig, connectionParams)
		if err != nil {
			return nil, err
		}

	}

	return extractors, nil
}

// NewExtractorGenerator returns an implementation of the extraction api
func NewExtractorGenerator() ExtractorGenerator {
	return &defaultExtractorGenerator{
		generate: extractors.NewInfluxExtractor,
	}
}
