package config

import (
	"fmt"
	"time"
)

const (
	acceptedTimeFormat = time.RFC3339
)

// MeasureExtraction holds config properties for a single measure
type MeasureExtraction struct {
	Database              string
	Measure               string
	From                  string
	To                    string
	ChunkSize             uint
	Limit                 uint
	DataChannelBufferSize uint
}

// NewMeasureExtractionConfig creates a new instance of MeasureExtraction while validating the fields
// 'chunkSize' must be positive, specifies the number of rows the database server sends to the client at once
// 'limit' if > 0 limits the number of points extracted from the measure, if == 0 all data is requested
// 'from' and 'to' are timestamps and optional. If specified request data only between these timescamps
func NewMeasureExtractionConfig(database, measure string, chunkSize, limit uint, from, to string) (*MeasureExtraction, error) {
	if database == "" || measure == "" {
		return nil, fmt.Errorf("database and measure can't be empty")
	}

	if chunkSize == 0 {
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
		Limit:     limit,
	}, nil
}
