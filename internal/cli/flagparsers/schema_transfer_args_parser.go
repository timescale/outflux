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

	strategyAsStr, _ := flags.GetString(SchemaStrategyFlag)
	var strategy schemaconfig.SchemaStrategy
	if strategy, err = schemaconfig.ParseStrategyString(strategyAsStr); err != nil {
		return nil, nil, err
	}

	quiet, err := flags.GetBool(QuietFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("value for the '%s' flag must be a true or false", QuietFlag)
	}
	return connectionArgs, &cli.MigrationConfig{
		OutputSchemaStrategy: strategy,
		Quiet:                quiet,
		SchemaOnly:           true,
		ChunkSize:            1,
	}, nil
}
