package flagparsers

import (
	ingestionConfig "github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// Flags used in outflux and their default values
const (
	InputServerFlag             = "input-server"
	InputUserFlag               = "input-user"
	InputPassFlag               = "input-pass"
	OutputConnFlag              = "output-conn"
	SchemaStrategyFlag          = "schema-strategy"
	CommitStrategyFlag          = "commit-strategy"
	OutputSchemaFlag            = "output-schema"
	FromFlag                    = "from"
	ToFlag                      = "to"
	LimitFlag                   = "limit"
	ChunkSizeFlag               = "chunk-size"
	QuietFlag                   = "quiet"
	DataBufferFlag              = "data-buffer"
	MaxParallelFlag             = "max-parallel"
	RollbackOnExternalErrorFlag = "rollback-on-external-error"
	BatchSizeFlag               = "batch-size"

	DefaultInputServer             = "http://localhost:8086"
	DefaultInputUser               = ""
	DefaultInputPass               = ""
	DefaultOutputConn              = "sslmode=disable"
	DefaultOutputSchema            = ""
	DefaultSchemaStrategy          = schemamanagement.CreateIfMissing
	DefaultCommitStrategy          = ingestionConfig.CommitOnEachBatch
	DefaultDataBufferSize          = 15000
	DefaultChunkSize               = 15000
	DefaultLimit                   = 0
	DefaultMaxParallel             = 2
	DefaultRollbackOnExternalError = true
	DefaultBatchSize               = 8000
)
