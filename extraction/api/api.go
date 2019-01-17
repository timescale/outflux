package api

import (
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
)

// API defines the methods for managing extractors
type API interface {
	CreateExtractors(config *config.ExtractorConfig) ([]extractors.InfluxExtractor, error)
}
