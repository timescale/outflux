package api

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
)

// ExtractorGenerator defines the methods for managing extractors
type ExtractorGenerator interface {
	CreateExtractors(config *config.ExtractorConfig) ([]extractors.InfluxExtractor, error)
}
