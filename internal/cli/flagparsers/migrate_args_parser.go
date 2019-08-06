package flagparsers

import (
	"fmt"
	"math"

	"github.com/spf13/pflag"
	"github.com/timescale/outflux/internal/cli"
	ingestionConfig "github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// FlagsToMigrateConfig extracts the config for running a migration from the flags of the command
func FlagsToMigrateConfig(flags *pflag.FlagSet, args []string) (*cli.ConnectionConfig, *cli.MigrationConfig, error) {
	connectionArgs, err := FlagsToConnectionConfig(flags, args)
	if err != nil {
		return nil, nil, err
	}

	strategyAsStr, _ := flags.GetString(SchemaStrategyFlag)
	var strategy schemaconfig.SchemaStrategy
	if strategy, err = schemaconfig.ParseStrategyString(strategyAsStr); err != nil {
		return nil, nil, err
	}

	commitStrategyAsStr, _ := flags.GetString(CommitStrategyFlag)
	var commitStrategy ingestionConfig.CommitStrategy
	if commitStrategy, err = ingestionConfig.ParseStrategyString(commitStrategyAsStr); err != nil {
		return nil, nil, err
	}

	limit, err := flags.GetUint64(LimitFlag)
	if err != nil {
		return nil, nil, err
	}

	chunkSize, err := flags.GetUint16(ChunkSizeFlag)
	if err != nil || chunkSize == 0 {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", ChunkSizeFlag, math.MaxUint16)
	}

	batchSize, err := flags.GetUint16(BatchSizeFlag)
	if err != nil || batchSize == 0 {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", ChunkSizeFlag, math.MaxUint16)
	}

	dataBuffer, err := flags.GetUint16(DataBufferFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be an integer >= 0 and < %d", DataBufferFlag, math.MaxUint16)
	}

	maxParallel, err := flags.GetUint8(MaxParallelFlag)
	if err != nil || maxParallel == 0 {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", MaxParallelFlag, math.MaxUint16)
	}

	quiet, err := flags.GetBool(QuietFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be a true or false", QuietFlag)
	}

	rollBack, err := flags.GetBool(RollbackOnExternalErrorFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be a true or false", RollbackOnExternalErrorFlag)
	}

	from, _ := flags.GetString(FromFlag)
	to, _ := flags.GetString(ToFlag)
	tagsAsJSON, _ := flags.GetBool(TagsAsJSONFlag)
	tagsColumn, _ := flags.GetString(TagsColumnFlag)
	if tagsAsJSON && tagsColumn == "" {
		return nil, nil, fmt.Errorf("When the '%s' flag is set, the '%s' must also have a value", TagsAsJSONFlag, TagsColumnFlag)
	}

	fieldsAsJSON, _ := flags.GetBool(FieldsAsJSONFlag)
	fieldsColumn, _ := flags.GetString(FieldsColumnFlag)
	if fieldsAsJSON && fieldsColumn == "" {
		return nil, nil, fmt.Errorf("When the '%s' flag is set, the '%s' must also have a value", FieldsAsJSONFlag, FieldsColumnFlag)
	}
	outputSchema, _ := flags.GetString(OutputSchemaFlag)
	rp, _ := flags.GetString(RetentionPolicyFlag)
	intToFloat, _ := flags.GetBool(MultishardIntFloatCast)
	chunkTimeInterval, _ := flags.GetString(ChunkTimeIntervalFlag)
	migrateArgs := &cli.MigrationConfig{
		RetentionPolicy:                      rp,
		OutputSchemaStrategy:                 strategy,
		OutputSchema:                         outputSchema,
		From:                                 from,
		To:                                   to,
		Limit:                                limit,
		ChunkSize:                            chunkSize,
		BatchSize:                            batchSize,
		DataBuffer:                           dataBuffer,
		MaxParallel:                          maxParallel,
		Quiet:                                quiet,
		RollbackAllMeasureExtractionsOnError: rollBack,
		CommitStrategy:                       commitStrategy,
		TagsAsJSON:                           tagsAsJSON,
		TagsCol:                              tagsColumn,
		FieldsAsJSON:                         fieldsAsJSON,
		FieldsCol:                            fieldsColumn,
		OnConflictConvertIntToFloat:          intToFloat,
		ChunkTimeInterval:                    chunkTimeInterval,
	}

	return connectionArgs, migrateArgs, nil
}
