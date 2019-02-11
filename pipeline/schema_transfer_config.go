package pipeline

import "github.com/timescale/outflux/schemamanagement"

type SchemaTransferConfig struct {
	Connection           *ConnectionConfig
	OutputSchemaStrategy schemamanagement.SchemaStrategy
	Quiet                bool
}
