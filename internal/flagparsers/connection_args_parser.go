package flagparsers

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/timescale/outflux/internal/pipeline"
)

// FlagsToConnectionConfig extracts flags related to establishing the connection to input and output database
func FlagsToConnectionConfig(flags *pflag.FlagSet, args []string) (*pipeline.ConnectionConfig, error) {
	if args[0] == "" {
		return nil, fmt.Errorf("input database name not specified")
	}

	inputUser, _ := flags.GetString(InputUserFlag)
	if inputUser == "" {
		return nil, fmt.Errorf("username to connect to the input database not specified, '%s' flag is required", InputUserFlag)
	}

	inputPass, _ := flags.GetString(InputPassFlag)
	if inputPass == "" {
		return nil, fmt.Errorf("password to connect to the input database not specified, '%s' flag is required", InputPassFlag)
	}

	inputHost, _ := flags.GetString(InputHostFlag)

	outputConnString, _ := flags.GetString(OutputConnFlag)
	schema, _ := flags.GetString(OutputSchemaFlag)
	return &pipeline.ConnectionConfig{
		InputDb:            args[0],
		InputMeasures:      args[1:],
		InputHost:          inputHost,
		InputUser:          inputUser,
		InputPass:          inputPass,
		OutputDbConnString: outputConnString,
		OutputSchema:       schema,
	}, nil
}
