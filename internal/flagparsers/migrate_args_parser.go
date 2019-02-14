package flagparsers

import (
	"fmt"
	"math"

	"github.com/spf13/pflag"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// FlagsToMigrateConfig extracts the config for running a migration from the flags of the command
func FlagsToMigrateConfig(flags *pflag.FlagSet, args []string) (*pipeline.MigrationConfig, error) {
	connectionArgs, err := FlagsToConnectionConfig(flags, args)
	if err != nil {
		return nil, err
	}

	strategyAsStr, err := flags.GetString(SchemaStrategyFlag)
	var strategy schemamanagement.SchemaStrategy
	if strategy, err = schemamanagement.ParseStrategyString(strategyAsStr); err != nil {
		return nil, err
	}

	limit, err := flags.GetUint64(LimitFlag)
	if err != nil {
		return nil, err
	}

	chunkSize, err := flags.GetUint16(ChunkSizeFlag)
	if err != nil || chunkSize == 0 {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", ChunkSizeFlag, math.MaxUint16)
	}

	dataBuffer, err := flags.GetUint16(DataBufferFlag)
	if err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer >= 0 and < %d", DataBufferFlag, math.MaxUint16)
	}

	maxParallel, err := flags.GetUint8(MaxParallelFlag)
	if err != nil || maxParallel == 0 {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", MaxParallelFlag, math.MaxUint16)
	}

	quiet, err := flags.GetBool(QuietFlag)
	if err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be a true or false", QuietFlag)
	}

	rollBack, err := flags.GetBool(RollbackOnExternalErrorFlag)
	if err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be a true or false", RollbackOnExternalErrorFlag)
	}

	from, _ := flags.GetString(FromFlag)
	to, _ := flags.GetString(ToFlag)
	migrateArgs := &pipeline.MigrationConfig{
		Connection:                           connectionArgs,
		OutputSchemaStrategy:                 strategy,
		From:                                 from,
		To:                                   to,
		Limit:                                limit,
		ChunkSize:                            chunkSize,
		DataBuffer:                           dataBuffer,
		MaxParallel:                          maxParallel,
		Quiet:                                quiet,
		RollbackAllMeasureExtractionsOnError: rollBack,
	}

	return migrateArgs, nil
}
