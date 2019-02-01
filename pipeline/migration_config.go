package pipeline

import (
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
)

type MigrationConfig struct {
	InputHost                            string
	InputDb                              string
	InputMeasures                        []string
	InputUser                            string
	InputPass                            string
	OutputHost                           string
	OutputDb                             string
	OutputSchema                         string
	OutputDbSslMode                      string
	OutputUser                           string
	OutputPassword                       string
	OutputSchemaStrategy                 ingestionConfig.SchemaStrategy
	From                                 string
	To                                   string
	Limit                                uint64
	ChunkSize                            uint16
	Quiet                                bool
	DataBuffer                           uint16
	MaxParallel                          uint8
	RollbackAllMeasureExtractionsOnError bool
}
