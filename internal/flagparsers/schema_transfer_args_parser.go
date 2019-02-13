package flagparsers

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
)

// FlagsToSchemaTransferConfig extracts the config for running schema transfer from the flags of the command
func FlagsToSchemaTransferConfig(flags *pflag.FlagSet, args []string) (*pipeline.SchemaTransferConfig, error) {
	connectionArgs, err := FlagsToConnectionConfig(flags, args)
	if err != nil {
		return nil, err
	}

	strategyAsStr, _ := flags.GetString(SchemaStrategyFlag)
	var strategy schemamanagement.SchemaStrategy
	if strategy, err = schemamanagement.ParseStrategyString(strategyAsStr); err != nil {
		return nil, err
	}

	quiet, err := flags.GetBool(QuietFlag)
	if err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be a true or false", QuietFlag)
	}
	return &pipeline.SchemaTransferConfig{
		Connection:           connectionArgs,
		OutputSchemaStrategy: strategy,
		Quiet:                quiet,
	}, nil
}
