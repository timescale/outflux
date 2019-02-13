package flagparsers

import "github.com/timescale/outflux/internal/schemamanagement"

// Flags used in outflux and their default values
const (
	InputHostFlag               = "input-host"
	InputUserFlag               = "input-user"
	InputPassFlag               = "input-pass"
	OutputHostFlag              = "output-host"
	OutputDbFlag                = "output-db"
	OutputDbSslModeFlag         = "output-db-ssl-mode"
	OutputUserFlag              = "output-user"
	OutputPasswordFlag          = "output-pass"
	SchemaStrategyFlag          = "schema-strategy"
	OutputSchemaFlag            = "output-schema"
	FromFlag                    = "from"
	ToFlag                      = "to"
	LimitFlag                   = "limit"
	ChunkSizeFlag               = "chunk-size"
	QuietFlag                   = "quiet"
	DataBufferFlag              = "data-buffer"
	MaxParallelFlag             = "max-parallel"
	RollbackOnExternalErrorFlag = "rollback-on-external-error"

	DefaultInputHost               = "http://localhost:8086"
	DefaultInputUser               = ""
	DefaultInputPass               = ""
	DefaultOutputHost              = "localhost:5432"
	DefaultSslMode                 = "disable"
	DefaultOutputSchema            = "public"
	DefaultSchemaStrategy          = schemamanagement.CreateIfMissing
	DefaultDataBufferSize          = 15000
	DefaultChunkSize               = 15000
	DefaultLimit                   = 0
	DefaultMaxParallel             = 2
	DefaultRollbackOnExternalError = true
)
