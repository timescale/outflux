package config

import (
	"fmt"
	"time"

	influxUtils "github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	acceptedTimeFormat = time.RFC3339
)

// ExtractorConfig holds config properties for an Extractor that can connect to InfluxDB
type ExtractorConfig struct {
	Connection *influxUtils.ConnectionParams
	Measures   []*MeasureExtraction
}

// MeasureExtraction holds config properties for a single measure
type MeasureExtraction struct {
	Database  string
	Measure   string
	From      string
	To        string
	ChunkSize int
}

// NewMeasureExtractionConfig creates a new instance of MeasureExtraction while validating the fields
func NewMeasureExtractionConfig(
	database string,
	measure string,
	from string,
	to string,
	chunkSize int) (*MeasureExtraction, error) {
	if database == "" || measure == "" {
		return nil, fmt.Errorf("database and measure can't be empty")
	}

	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be > 0")
	}

	_, formatError := time.Parse(acceptedTimeFormat, from)
	if from != "" && formatError != nil {
		return nil, fmt.Errorf("'from' time must be formatted as %s", acceptedTimeFormat)
	}

	_, formatError = time.Parse(acceptedTimeFormat, to)
	if to != "" && formatError != nil {
		return nil, fmt.Errorf("'to' time must be formatted as %s", acceptedTimeFormat)
	}

	return &MeasureExtraction{
		Database:  database,
		Measure:   measure,
		From:      from,
		To:        to,
		ChunkSize: chunkSize,
	}, nil
}
