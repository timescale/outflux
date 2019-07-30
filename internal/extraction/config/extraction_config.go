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
	Database                    string
	Measure                     string
	From                        string
	To                          string
	ChunkSize                   uint16
	Limit                       uint64
	SchemaOnly                  bool
	RetentionPolicy             string
	OnConflictConvertIntToFloat bool
}

// ValidateMeasureExtractionConfig validates the fields
// 'chunkSize' must be positive, specifies the number of rows the database server sends to the client at once
// 'limit' if > 0 limits the number of points extracted from the measure, if == 0 all data is requested
// 'from' and 'to' are timestamps and optional. If specified request data only between these timescamps
func ValidateMeasureExtractionConfig(config *MeasureExtraction) error {
	if config.Database == "" || config.Measure == "" {
		return fmt.Errorf("database and measure can't be empty")
	}

	if config.ChunkSize == 0 {
		return fmt.Errorf("chunk size must be > 0")
	}

	_, formatError := time.Parse(acceptedTimeFormat, config.From)
	if config.From != "" && formatError != nil {
		return fmt.Errorf("'from' time must be formatted as %s", acceptedTimeFormat)
	}

	_, formatError = time.Parse(acceptedTimeFormat, config.To)
	if config.To != "" && formatError != nil {
		return fmt.Errorf("'to' time must be formatted as %s", acceptedTimeFormat)
	}

	return nil
}

// ExtractionConfig combines everything needed to create and start an Extractor
type ExtractionConfig struct {
	ExtractorID       string
	MeasureExtraction *MeasureExtraction
	DataBufferSize    uint16
}
