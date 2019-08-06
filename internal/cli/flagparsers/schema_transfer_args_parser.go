package flagparsers

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// FlagsToSchemaTransferConfig extracts the config for running schema transfer from the flags of the command
func FlagsToSchemaTransferConfig(flags *pflag.FlagSet, args []string) (*cli.ConnectionConfig, *cli.MigrationConfig, error) {
	connectionArgs, err := FlagsToConnectionConfig(flags, args)
	if err != nil {
		return nil, nil, err
	}

	retentionPolicy, _ := flags.GetString(RetentionPolicyFlag)
	strategyAsStr, _ := flags.GetString(SchemaStrategyFlag)
	var strategy schemaconfig.SchemaStrategy
	if strategy, err = schemaconfig.ParseStrategyString(strategyAsStr); err != nil {
		return nil, nil, err
	}

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

	quiet, err := flags.GetBool(QuietFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be a true or false", QuietFlag)
	}
	outputSchema, _ := flags.GetString(OutputSchemaFlag)
	intToFloat, _ := flags.GetBool(MultishardIntFloatCast)
	chunkTimeInterval, _ := flags.GetString(ChunkTimeIntervalFlag)
	return connectionArgs, &cli.MigrationConfig{
		RetentionPolicy:             retentionPolicy,
		OutputSchema:                outputSchema,
		OutputSchemaStrategy:        strategy,
		Quiet:                       quiet,
		SchemaOnly:                  true,
		ChunkSize:                   1,
		TagsAsJSON:                  tagsAsJSON,
		TagsCol:                     tagsColumn,
		FieldsAsJSON:                fieldsAsJSON,
		FieldsCol:                   fieldsColumn,
		OnConflictConvertIntToFloat: intToFloat,
		ChunkTimeInterval:           chunkTimeInterval,
	}, nil
}
