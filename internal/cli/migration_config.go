package cli

import (
	ingestionConf "github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// MigrationConfig contains the configurable parameters for migrating an InfluxDB to TimescaleDB
type MigrationConfig struct {
	OutputSchemaStrategy                 schemaconfig.SchemaStrategy
	From                                 string
	To                                   string
	Limit                                uint64
	ChunkSize                            uint16
	BatchSize                            uint16
	Quiet                                bool
	DataBuffer                           uint16
	MaxParallel                          uint8
	RollbackAllMeasureExtractionsOnError bool
	CommitStrategy                       ingestionConf.CommitStrategy
	SchemaOnly                           bool
	TagsAsJSON                           bool
	TagsCol                              string
	FieldsAsJSON                         bool
	FieldsCol                            string
}
