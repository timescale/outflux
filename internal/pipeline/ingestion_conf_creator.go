package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/ingestion"
)

type ingestionConfCreator interface {
	create(pipeNum int, conf *MigrationConfig) *ingestion.IngestorConfig
}

type defaultIngestionConfCreator struct {
}

func (s *defaultIngestionConfCreator) create(pipeNum int, conf *MigrationConfig) *ingestion.IngestorConfig {
	return &ingestion.IngestorConfig{
		IngestorID:              fmt.Sprintf("pipe_%d_ing", pipeNum),
		BatchSize:               conf.BatchSize,
		RollbackOnExternalError: conf.RollbackAllMeasureExtractionsOnError,
		CommitStrategy:          conf.CommitStrategy,
	}
}
