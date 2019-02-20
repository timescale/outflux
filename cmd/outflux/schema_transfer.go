package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/jackc/pgx"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/flagparsers"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
	tsSchema "github.com/timescale/outflux/internal/schemamanagement/ts"
)

func initSchemaTransferCmd() *cobra.Command {
	schemaTransferCmd := &cobra.Command{
		Use:  "schema-transfer database [measure1 measure2 ...]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			schemaTransferArgs, err := flagparsers.FlagsToSchemaTransferConfig(cmd.Flags(), args)
			if err != nil {
				log.Fatal(err)
				return
			}

			if schemaTransferArgs.Quiet {
				log.SetFlags(0)
				log.SetOutput(ioutil.Discard)
			}

			_, err = transferSchema(app, schemaTransferArgs)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	flagparsers.AddConnectionFlagsToCmd(schemaTransferCmd)
	schemaTransferCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	return schemaTransferCmd
}

func transferSchema(app *appContext, args *pipeline.SchemaTransferConfig) ([]*idrf.DataSetInfo, error) {
	influxClient, err := createInfluxClient(args.Connection, app.ics)
	if err != nil {
		return nil, fmt.Errorf("could not craete influx client\n%v", err)
	}

	defer influxClient.Close()

	startTime := time.Now()

	influxDb := args.Connection.InputDb
	log.Printf("Selected input database: %s\n", influxDb)
	influxMeasures := args.Connection.InputMeasures

	var discoveredDataSets []*idrf.DataSetInfo
	if len(influxMeasures) == 0 {
		discoveredDataSets, err = discoverSchemaForDatabase(app, args.Connection, influxClient)
	} else {
		discoveredDataSets, err = discoverSchemaForMeasures(app, args.Connection, influxClient)
	}

	if err != nil {
		return nil, err
	}

	log.Println("Extracted data sets schema. Preparing output database")
	dbConn, err := app.tscs.NewConnection(args.Connection.OutputDbConnString)
	if err != nil {
		return nil, fmt.Errorf("could not open connection to output db\n%v", err)
	}

	defer dbConn.Close()

	for _, dataSet := range discoveredDataSets {
		dataSet.DataSetSchema = args.Connection.OutputSchema
		err := prepareOutputDataSet(dataSet, args.OutputSchemaStrategy, dbConn)
		if err != nil {
			return nil, fmt.Errorf("could not prepare output data set '%s'\n%v", dataSet.DataSetName, err)
		}
	}

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Schema Transfer complete in: %.3f seconds\n", executionTime)
	return discoveredDataSets, nil
}

func prepareOutputDataSet(
	dataSet *idrf.DataSetInfo,
	strategy schemamanagement.SchemaStrategy,
	dbConn *pgx.Conn) error {
	tsSchemaManager := tsSchema.NewTSSchemaManager(dbConn)
	return tsSchemaManager.PrepareDataSet(dataSet, strategy)
}
