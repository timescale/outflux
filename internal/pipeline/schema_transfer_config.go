package pipeline

import "github.com/timescale/outflux/internal/schemamanagement"

type SchemaTransferConfig struct {
	Connection           *ConnectionConfig
	OutputSchemaStrategy schemamanagement.SchemaStrategy
	Quiet                bool
}
