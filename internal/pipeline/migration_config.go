package pipeline

import (
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// MigrationConfig contains the configurable parameters for migrating an InfluxDB to TimescaleDB
type MigrationConfig struct {
	Connection                           *ConnectionConfig
	OutputSchemaStrategy                 schemamanagement.SchemaStrategy
	From                                 string
	To                                   string
	Limit                                uint64
	ChunkSize                            uint16
	BatchSize                            uint16
	Quiet                                bool
	DataBuffer                           uint16
	MaxParallel                          uint8
	RollbackAllMeasureExtractionsOnError bool
	CommitStrategy                       ingestion.CommitStrategy
}

// ToSchemaTransferConfig transforms the migration command config to schema transfer config
func (m *MigrationConfig) ToSchemaTransferConfig() *SchemaTransferConfig {
	return &SchemaTransferConfig{
		Connection:           m.Connection,
		OutputSchemaStrategy: m.OutputSchemaStrategy,
		Quiet:                m.Quiet,
	}
}
