package config

import (
	"fmt"
	"time"

	"github.com/timescale/outflux/schemadiscovery/clientutils"
	"github.com/timescale/outflux/utils"
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

	if _, err := utils.SafeCastUInt(config.ChunkSize); err != nil {
		return err
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

// Config combines everthing needed to create and start an Extractor
type Config struct {
	ExtractorID       string
	MeasureExtraction *MeasureExtraction
	Connection        *clientutils.ConnectionParams
}

// Config combines everthing needed to create and start an Extractor
type Config struct {
	ExtractorID       string
	MeasureExtraction *MeasureExtraction
	Connection        *clientutils.ConnectionParams
}
