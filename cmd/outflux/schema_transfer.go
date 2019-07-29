package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/cli"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/cli/flagparsers"
)

func initSchemaTransferCmd() *cobra.Command {
	schemaTransferCmd := &cobra.Command{
		Use:   "schema-transfer database [measure1 measure2 ...]",
		Short: "Discover the schema of measurements and validate or prepare a TimescaleDB hyper-table with the discovered schema",
		Long:  "Discover the schema of measurements and validate or prepare a TimescaleDB hyper-table with the discovered schema",
		Args:  cobra.MinimumNArgs(1),
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
	schemaTransferCmd.PersistentFlags().String(flagparsers.RetentionPolicyFlag, flagparsers.DefaultRetentionPolicy, "The retention policy to select the fields and tags from")
	schemaTransferCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	schemaTransferCmd.PersistentFlags().Bool(flagparsers.TagsAsJSONFlag, flagparsers.DefaultTagsAsJSON, "If this flag is set to true, then the Tags of the influx measures being exported will be combined into a single JSONb column in Timescale")
	schemaTransferCmd.PersistentFlags().String(flagparsers.TagsColumnFlag, flagparsers.DefaultTagsColumn, "When "+flagparsers.TagsAsJSONFlag+" is set, this column specifies the name of the JSON column for the tags")
	schemaTransferCmd.PersistentFlags().Bool(flagparsers.FieldsAsJSONFlag, flagparsers.DefaultFieldsAsJSON, "If this flag is set to true, then the Fields of the influx measures being exported will be combined into a single JSONb column in Timescale")
	schemaTransferCmd.PersistentFlags().String(flagparsers.FieldsColumnFlag, flagparsers.DefaultFieldsColumn, "When "+flagparsers.FieldsAsJSONFlag+" is set, this column specifies the name of the JSON column for the fields")
	schemaTransferCmd.PersistentFlags().String(flagparsers.OutputSchemaFlag, flagparsers.DefaultOutputSchema, "The schema of the output database that the data will be inserted into")

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

	// connect to input and output database
	infConn, pgConn, err := openConnections(app, connArgs)
	if err != nil {
		return fmt.Errorf("could not open connections to input and output database\n%v", err)
	}
	defer infConn.Close()
	defer pgConn.Close()

	// transfer the schema for all measures
	if len(connArgs.InputMeasures) == 0 {
		connArgs.InputMeasures, err = discoverMeasures(app, infConn, connArgs.InputDb, args.RetentionPolicy)
		if err != nil {
			return fmt.Errorf("could not discover the available measures for the input db '%s'", connArgs.InputDb)
		}
	}

	for _, measure := range connArgs.InputMeasures {
		err := transfer(app, connArgs.InputDb, args, infConn, pgConn, measure)
		if err != nil {
			return fmt.Errorf("could not transfer schema for measurement '%s'\n%v", measure, err)
		}
	}

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Schema Transfer complete in: %.3f seconds\n", executionTime)
	return nil
}

func discoverMeasures(app *appContext, influxConn influx.Client, db, rp string) ([]string, error) {
	schemaManager := app.schemaManagerService.Influx(influxConn, db, rp)
	return schemaManager.DiscoverDataSets()
}

func transfer(
	app *appContext,
	inputDb string,
	args *cli.MigrationConfig,
	infConn influx.Client,
	pgConn *pgx.Conn,
	measure string) error {

	pipe, err := app.pipeService.Create(infConn, pgConn, measure, inputDb, args)
	if err != nil {
		return fmt.Errorf("could not create execution pipeline for measure '%s'\n%v", measure, err)
	}

	log.Printf("%s starting execution\n", pipe.ID())
	return pipe.Run()
}
