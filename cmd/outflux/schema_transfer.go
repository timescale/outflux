package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/timescale/outflux/internal/connections"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/flagparsers"
	"github.com/timescale/outflux/internal/pipeline"
	influxSchema "github.com/timescale/outflux/internal/schemamanagement/influx"
)

func initSchemaTransferCmd() *cobra.Command {
	schemaTransferCmd := &cobra.Command{
		Use:  "schema-transfer database [measure1 measure2 ...]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			connArgs, schemaTransferArgs, err := flagparsers.FlagsToSchemaTransferConfig(cmd.Flags(), args)
			if err != nil {
				log.Fatal(err)
				return
			}

			err = transferSchema(app, connArgs, schemaTransferArgs)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	flagparsers.AddConnectionFlagsToCmd(schemaTransferCmd)
	schemaTransferCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	return schemaTransferCmd
}

func transferSchema(app *appContext, connArgs *pipeline.ConnectionConfig, args *pipeline.SchemaTransferConfig) error {
	if args.Quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	startTime := time.Now()
	influxDb := connArgs.InputDb
	log.Printf("Selected input database: %s\n", influxDb)
	var err error
	// transfer the schema for all measures
	if len(connArgs.InputMeasures) == 0 {
		connArgs.InputMeasures, err = discoverMeasures(app, connArgs)
		if err != nil {
			return fmt.Errorf("could not discover the available measures for the input db '%s'", connArgs.InputDb)
		}
	}

	// SELECT * FROM measure LIMIT 0
	migrateConfig := args.ToMigrationConfig()
	pipes := app.pipeService.Create(connArgs, migrateConfig)
	for _, pipe := range pipes {
		err := pipe.Run()
		if err != nil {
			return fmt.Errorf("could not transfer schema for one of the measures\n%v", err)
		}
	}

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Schema Transfer complete in: %.3f seconds\n", executionTime)
	return nil
}

func discoverMeasures(app *appContext, connArgs *pipeline.ConnectionConfig) ([]string, error) {
	client, err := app.ics.NewConnection(&connections.InfluxConnectionParams{
		Server:   connArgs.InputHost,
		Username: connArgs.InputUser,
		Password: connArgs.InputPass,
		Database: connArgs.InputDb,
	})
	if err != nil {
		return nil, err
	}

	schemaManager := influxSchema.NewInfluxSchemaManager(client, app.influxQueryService, connArgs.InputDb)
	client.Close()
	return schemaManager.DiscoverDataSets()
}
