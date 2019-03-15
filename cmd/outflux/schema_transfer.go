package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/connections"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/cli/flagparsers"
)

func initSchemaTransferCmd() *cobra.Command {
	schemaTransferCmd := &cobra.Command{
		Use:  "schema-transfer database [measure1 measure2 ...]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			connArgs, migArgs, err := flagparsers.FlagsToSchemaTransferConfig(cmd.Flags(), args)
			if err != nil {
				log.Fatal(err)
				return
			}

			err = transferSchema(app, connArgs, migArgs)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	flagparsers.AddConnectionFlagsToCmd(schemaTransferCmd)
	schemaTransferCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	schemaTransferCmd.PersistentFlags().Bool(flagparsers.TagsAsJSONFlag, flagparsers.DefaultTagsAsJSON, "If this flag is set to true, then the Tags of the influx measures being exported will be combined into a single JSONb column in Timescale")
	schemaTransferCmd.PersistentFlags().String(flagparsers.TagsColumnFlag, flagparsers.DefaultTagsColumn, "When "+flagparsers.TagsAsJSONFlag+" is set, this column specifies the name of the JSON column for the tags")
	schemaTransferCmd.PersistentFlags().Bool(flagparsers.FieldsAsJSONFlag, flagparsers.DefaultFieldsAsJSON, "If this flag is set to true, then the Fields of the influx measures being exported will be combined into a single JSONb column in Timescale")
	schemaTransferCmd.PersistentFlags().String(flagparsers.FieldsColumnFlag, flagparsers.DefaultFieldsColumn, "When "+flagparsers.FieldsAsJSONFlag+" is set, this column specifies the name of the JSON column for the fields")
	return schemaTransferCmd
}

func transferSchema(app *appContext, connArgs *cli.ConnectionConfig, args *cli.MigrationConfig) error {
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

	for _, measure := range connArgs.InputMeasures {
		err := routine(app, connArgs, args, measure)
		if err != nil {
			return fmt.Errorf("could not transfer schema for measurement '%s'\n%v", measure, err)
		}
	}

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Schema Transfer complete in: %.3f seconds\n", executionTime)
	return nil
}

func discoverMeasures(app *appContext, connArgs *cli.ConnectionConfig) ([]string, error) {
	client, err := app.ics.NewConnection(&connections.InfluxConnectionParams{
		Server:   connArgs.InputHost,
		Username: connArgs.InputUser,
		Password: connArgs.InputPass,
		Database: connArgs.InputDb,
	})
	if err != nil {
		return nil, err
	}

	schemaManager := app.schemaManagerService.Influx(client, connArgs.InputDb)
	client.Close()
	return schemaManager.DiscoverDataSets()
}

func routine(
	app *appContext,
	connArgs *cli.ConnectionConfig,
	args *cli.MigrationConfig,
	measure string) error {

	infConn, pgConn, err := openConnections(app, connArgs)
	if err != nil {
		return fmt.Errorf("could not open connections to input and output database\n%v", err)
	}
	defer infConn.Close()
	defer pgConn.Close()

	pipe, err := app.pipeService.Create(infConn, pgConn, measure, connArgs, args)
	if err != nil {
		return fmt.Errorf("could not create execution pipeline for measure '%s'\n%v", measure, err)
	}

	log.Printf("%s starting execution\n", pipe.ID())
	return pipe.Run()
}
