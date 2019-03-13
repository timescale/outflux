package cli

import (
	"fmt"

	"github.com/timescale/outflux/internal/extraction/config"
)

const (
	extractorIDTemplate = "%s_ext"
)

type extractionConfCreator interface {
	create(pipeID string, db, measure string, conf *MigrationConfig) *config.ExtractionConfig
}

type defaultExtractionConfCreator struct{}

func (d *defaultExtractionConfCreator) create(pipeID, db, measure string, conf *MigrationConfig) *config.ExtractionConfig {
	measureExtractionConf := &config.MeasureExtraction{
		Database:   db,
		Measure:    measure,
		From:       conf.From,
		To:         conf.To,
		ChunkSize:  conf.ChunkSize,
		Limit:      conf.Limit,
		SchemaOnly: conf.SchemaOnly,
	}

	ex := &config.ExtractionConfig{
		ExtractorID:       fmt.Sprintf(extractorIDTemplate, pipeID),
		MeasureExtraction: measureExtractionConf,
		DataBufferSize:    conf.DataBuffer,
	}

	return ex
}
