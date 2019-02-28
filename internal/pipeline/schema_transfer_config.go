package pipeline

import (
	"github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type SchemaTransferConfig struct {
	OutputSchemaStrategy schemamanagement.SchemaStrategy
	Quiet                bool
}

// ToMigrationConfig transforms the schema transfer command config to migration config with limit 0
func (s *SchemaTransferConfig) ToMigrationConfig() *MigrationConfig {
	return &MigrationConfig{
		OutputSchemaStrategy:                 s.OutputSchemaStrategy,
		Limit:                                0,
		ChunkSize:                            1,
		BatchSize:                            1,
		Quiet:                                s.Quiet,
		DataBuffer:                           1,
		MaxParallel:                          1,
		RollbackAllMeasureExtractionsOnError: true,
		CommitStrategy:                       config.CommitOnEachBatch,
	}
}
