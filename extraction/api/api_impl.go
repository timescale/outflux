package api

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
)

type defaultExtractorGenerator struct {
	generator extractors.InfluxExtractorGenerator
}

func (api *defaultExtractorGenerator) CreateExtractors(config *config.ExtractorConfig) ([]extractors.InfluxExtractor, error) {
	connectionParams := config.Connection
	extractors := make([]extractors.InfluxExtractor, len(config.Measures))
	for index, measureConfig := range config.Measures {
		var err error
		extractors[index], err = api.generator.Generate(measureConfig, connectionParams)
		if err != nil {
			return nil, err
		}

	}

	return extractors, nil
}

// NewExtractorGenerator returns an implementation of the extraction api
func NewExtractorGenerator() ExtractorGenerator {
	return &defaultExtractorGenerator{
		generator: extractors.NewInfluxExtractorGenerator(),
	}
}

// NewExtractorGeneratorWith returns an implementation of the extraction api, with supplied dependencies
func NewExtractorGeneratorWith(generator extractors.InfluxExtractorGenerator) ExtractorGenerator {
	return &defaultExtractorGenerator{
		generator: generator,
	}
}
