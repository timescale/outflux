package cmd

import (
	"fmt"
	"math"
	"strconv"

	"github.com/spf13/cobra"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/pipeline"
)

func flagsToConfig(cmd *cobra.Command, args []string) (*pipeline.MigrationConfig, error) {
	if args[0] == "" {
		return nil, fmt.Errorf("input database name not specified")
	}

	inputHost := cmd.Flag(inputHostFlag).Value.String()
	if inputHost == "" {
		inputHost = defaultInputHost
	}

	inputUser := cmd.Flag(inputUserFlag).Value.String()
	if inputUser == "" {
		return nil, fmt.Errorf("username to connect to the input database not specified, '%s' flag is required", inputUserFlag)
	}

	inputPass := cmd.Flag(inputPassFlag).Value.String()
	if inputPass == "" {
		return nil, fmt.Errorf("password to connect to the input database not specified, '%s' flag is required", inputPassFlag)
	}

	outputHost := cmd.Flag(outputHostFlag).Value.String()
	if outputHost == "" {
		outputHost = defaultOutputHost
	}

	outputDB := cmd.Flag(outputDbFlag).Value.String()
	if outputDB == "" {
		return nil, fmt.Errorf("output database name not specified, '%s' flag is required", outputDbFlag)
	}

	sslMode := cmd.Flag(outputDbSslModeFlag).Value.String()
	if sslMode == "" {
		sslMode = defaultSslMode
	}

	outputUser := cmd.Flag(outputUserFlag).Value.String()
	if outputUser == "" {
		return nil, fmt.Errorf("username for output database not specified, '%s' flag is required", outputUserFlag)
	}

	outputPass := cmd.Flag(outputPasswordFlag).Value.String()
	if outputPass == "" {
		return nil, fmt.Errorf("password for output database not specified, '%s' flag is required", outputPasswordFlag)
	}

	strategyAsStr := cmd.Flag(schemaStrategyFlag).Value.String()
	var strategy ingestionConfig.SchemaStrategy
	var err error
	if strategyAsStr == "" {
		strategy = defaultSchemaStrategy
	} else if strategy, err = ingestionConfig.ParseStrategyString(strategyAsStr); err != nil {
		return nil, err
	}

	limitAsStr := cmd.Flag(limitFlag).Value.String()
	var limit uint64
	if limitAsStr == "" {
		limit = defaultLimit
	} else if limit, err = strconv.ParseUint(limitAsStr, 10, 64); err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer >= 0", limitFlag)
	}

	chunkSizeAsStr := cmd.Flag(chunkSizeFlag).Value.String()
	var chunkSize uint64
	if chunkSizeAsStr == "" {
		chunkSize = defaultChunkSize
	} else if chunkSize, err = strconv.ParseUint(chunkSizeAsStr, 10, 16); err != nil || chunkSize == 0 {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", limitFlag, math.MaxUint16)
	}

	dataBufferAsStr := cmd.Flag(dataBufferFlag).Value.String()
	var dataBufferSize uint64
	if dataBufferAsStr == "" {
		dataBufferSize = defaultDataBufferSize
	} else if dataBufferSize, err = strconv.ParseUint(dataBufferAsStr, 10, 16); err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer >= 0 and < %d", dataBufferFlag, math.MaxUint16)
	}

	maxParallelAsStr := cmd.Flag(maxParallelFlag).Value.String()
	var maxParallel uint64
	if maxParallelAsStr == "" {
		maxParallel = defaultMaxParallel
	} else if maxParallel, err = strconv.ParseUint(maxParallelAsStr, 10, 8); err != nil || maxParallel == 0 {
		return nil, fmt.Errorf("value for the '%s' flag must be an integer > 0 and < %d", maxParallelFlag, math.MaxUint16)
	}

	outputSchema := cmd.Flag(outputSchemaFlag).Value.String()
	if outputSchema == "" {
		outputSchema = defaultOutputSchema
	}

	quietAsStr := cmd.Flag(quietFlag).Value.String()
	var quiet bool
	if quiet, err = strconv.ParseBool(quietAsStr); err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be a true or false", quietFlag)
	}

	rollBackAsStr := cmd.Flag(rollbackOnExternalErrorFlag).Value.String()
	var rollBack bool
	if rollBack, err = strconv.ParseBool(rollBackAsStr); err != nil {
		return nil, fmt.Errorf("value for the '%s' flag must be a true or false", rollbackOnExternalErrorFlag)
	}
	migrateArgs := &pipeline.MigrationConfig{
		InputDb:                              args[0],
		InputMeasures:                        args[1:],
		InputHost:                            inputHost,
		InputUser:                            inputUser,
		InputPass:                            inputPass,
		OutputHost:                           outputHost,
		OutputDb:                             outputDB,
		OutputSchema:                         outputSchema,
		OutputDbSslMode:                      sslMode,
		OutputUser:                           outputUser,
		OutputPassword:                       outputPass,
		OutputSchemaStrategy:                 strategy,
		From:                                 cmd.Flag(fromFlag).Value.String(),
		To:                                   cmd.Flag(toFlag).Value.String(),
		Limit:                                limit,
		ChunkSize:                            uint16(chunkSize),
		DataBuffer:                           uint16(dataBufferSize),
		MaxParallel:                          uint8(maxParallel),
		Quiet:                                quiet,
		RollbackAllMeasureExtractionsOnError: rollBack,
	}

	return migrateArgs, nil
}
