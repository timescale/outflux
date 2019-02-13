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

	outputDB, _ := flags.GetString(OutputDbFlag)
	if outputDB == "" {
		return nil, fmt.Errorf("output database name not specified, '%s' flag is required", OutputDbFlag)
	}

	outputUser, _ := flags.GetString(OutputUserFlag)
	if outputUser == "" {
		return nil, fmt.Errorf("username for output database not specified, '%s' flag is required", OutputUserFlag)
	}

	outputPass, _ := flags.GetString(OutputPasswordFlag)
	if outputPass == "" {
		return nil, fmt.Errorf("password for output database not specified, '%s' flag is required", OutputPasswordFlag)
	}

	inputHost, _ := flags.GetString(InputHostFlag)
	outputHost, _ := flags.GetString(OutputHostFlag)
	outputSchema, _ := flags.GetString(OutputSchemaFlag)
	sslMode, _ := flags.GetString(OutputDbSslModeFlag)
	return &pipeline.ConnectionConfig{
		InputDb:         args[0],
		InputMeasures:   args[1:],
		InputHost:       inputHost,
		InputUser:       inputUser,
		InputPass:       inputPass,
		OutputHost:      outputHost,
		OutputDb:        outputDB,
		OutputSchema:    outputSchema,
		OutputDbSslMode: sslMode,
		OutputUser:      outputUser,
		OutputPassword:  outputPass,
	}, nil
}
