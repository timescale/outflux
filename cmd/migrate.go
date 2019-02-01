package cmd

import (
	"fmt"

	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/schemadiscovery/clientutils"

	"github.com/timescale/outflux/idrf"

	"github.com/spf13/cobra"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/utils"
)

const (
	inputHostFlag               = "input-host"
	inputUserFlag               = "input-user"
	inputPassFlag               = "input-pass"
	outputHostFlag              = "output-host"
	outputDbFlag                = "output-db"
	outputDbSslModeFlag         = "output-db-ssl-mode"
	outputUserFlag              = "output-user"
	outputPasswordFlag          = "output-pass"
	schemaStrategyFlag          = "schema-strategy"
	outputSchemaFlag            = "output-schema"
	fromFlag                    = "from"
	toFlag                      = "to"
	limitFlag                   = "limit"
	chunkSizeFlag               = "chunk-size"
	quietFlag                   = "quiet"
	dataBufferFlag              = "data-buffer"
	maxParallelFlag             = "max-parallel"
	rollbackOnExternalErrorFlag = "rollback-on-external-error"

	defaultInputHost               = "http://localhost:8086"
	defaultInputUser               = ""
	defaultInputPass               = ""
	defaultOutputHost              = "localhost:5432"
	defaultSslMode                 = "disable"
	defaultOutputSchema            = "public"
	defaultSchemaStrategy          = ingestionConfig.ValidateOnly
	defaultDataBufferSize          = 10000
	defaultChunkSize               = 10000
	defaultLimit                   = 0
	defaultMaxParallel             = 2
	defaultRollbackOnExternalError = true
)

var migrateCmd = &cobra.Command{
	Use:  "migrate database [measure1 measure2 ...]",
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		migrateArgs, err := flagsToConfig(cmd, args)
		if err != nil {
			panic(err)
		}

		err = migrate(migrateArgs)
		if err != nil {
			panic(err)
		}
	},
}

func migrate(args *pipeline.MigrationConfig) error {
	logger := utils.NewLogger(args.Quiet)

	influxDb := args.InputDb
	logger.Log("Selected input database: " + influxDb)
	influxMeasures := args.InputMeasures

	var discoveredDataSets []*idrf.DataSetInfo
	var err error
	if len(influxMeasures) == 0 {
		discoveredDataSets, err = discoverSchemaForDatabase(args, logger)
	} else {
		discoveredDataSets, err = discoverSchemaForMeasures(args, logger)
	}

	if err != nil {
		return err
	}

	logger.Log(fmt.Sprintf("Creating %d execution pipelines", len(influxMeasures)))
	pipelines := pipeline.CreatePipelines(discoveredDataSets, args)
	for _, pipe := range pipelines {
		err = pipe.Start()
		if err != nil {
			logger.Log("Pipeline completed with error")
			return err
		}
	}

	logger.Log("All pipelines completed")
	return nil
}

func discoverSchemaForDatabase(args *pipeline.MigrationConfig, logger utils.Logger) ([]*idrf.DataSetInfo, error) {
	logger.Log("All measurements selected for exporting")
	schemaExplorer := schemadiscovery.NewSchemaExplorer()
	influxConnectionParams := &clientutils.ConnectionParams{
		Server:   args.InputHost,
		Username: args.InputUser,
		Password: args.InputPass,
	}
	discoveredDataSets, err := schemaExplorer.InfluxDatabaseSchema(influxConnectionParams, args.InputDb)
	if err != nil {
		logger.Log("Couldn't discover the database schema")
		return nil, err
	}

	return discoveredDataSets, nil
}

func discoverSchemaForMeasures(args *pipeline.MigrationConfig, logger utils.Logger) ([]*idrf.DataSetInfo, error) {
	logger.Log(fmt.Sprintf("Selected measurements for exporting: %v", args.InputMeasures))
	schemaExplorer := schemadiscovery.NewSchemaExplorer()
	influxConnParams := &clientutils.ConnectionParams{
		Server:   args.InputHost,
		Username: args.InputUser,
		Password: args.InputPass,
	}

	discoveredDataSets := make([]*idrf.DataSetInfo, len(args.InputMeasures))
	var err error
	for i, measureName := range args.InputMeasures {
		discoveredDataSets[i], err = schemaExplorer.InfluxMeasurementSchema(influxConnParams, args.InputDb, measureName)
		if err != nil {
			logger.Log(fmt.Sprintf("Could not discover schema for measurement: %s", measureName))
			return nil, err
		}
	}

	return discoveredDataSets, nil
}
