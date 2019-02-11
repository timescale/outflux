package cmd

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/timescale/outflux/connections"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/cmd/flagparsers"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/schemamanagement"
	tsSchema "github.com/timescale/outflux/schemamanagement/ts"
)

func initSchemaTransferCmd() *cobra.Command {
	schemaTransferCmd := &cobra.Command{
		Use:  "schema-transfer database [measure1 measure2 ...]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			schemaTransferArgs, err := flagparsers.FlagsToSchemaTransferConfig(cmd.Flags(), args)
			if err != nil {
				panic(err)
			}

			if schemaTransferArgs.Quiet {
				log.SetFlags(0)
				log.SetOutput(ioutil.Discard)
			}

			_, error := transferSchema(app, schemaTransferArgs)
			if error != nil {
				panic(err)
			}
		},
	}

	flagparsers.AddConnectionFlagsToCmd(schemaTransferCmd)
	schemaTransferCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	return schemaTransferCmd
}

func transferSchema(app *appContext, args *pipeline.SchemaTransferConfig) ([]*idrf.DataSetInfo, error) {
	influxClient, err := createInfluxClient(args.Connection, app.influxConnectionService)
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

	log.Println("Extracted data sets schema. Prepairing output database")
	tsConnectionParams := tsConnParams(args.Connection)
	dbConn, err := app.tsConnectionService.NewConnection(tsConnectionParams)
	if err != nil {
		return nil, fmt.Errorf("could not open connection to output db\n%v", err)
	}

	defer dbConn.Close()

	for _, dataSet := range discoveredDataSets {
		err := prepareOutputDataSet(app, dataSet, args.OutputSchemaStrategy, dbConn)
		if err != nil {
			return nil, fmt.Errorf("could not prepare output data set '%s'\n%v", dataSet.DataSetName, err)
		}
	}

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Schema Transfer complete in: %.3f seconds\n", executionTime)
	return discoveredDataSets, nil
}

func prepareOutputDataSet(
	app *appContext,
	dataSet *idrf.DataSetInfo,
	strategy schemamanagement.SchemaStrategy,
	dbConn *sql.DB) error {
	tsSchemaManager := tsSchema.NewTSSchemaManager(dbConn)
	return tsSchemaManager.PrepareDataSet(dataSet, strategy)
}

func tsConnParams(conf *pipeline.ConnectionConfig) *connections.TSConnectionParams {
	additionalConnParams := make(map[string]string)
	additionalConnParams["sslmode"] = conf.OutputDbSslMode
	return &connections.TSConnectionParams{
		Server:               conf.OutputHost,
		Username:             conf.OutputUser,
		Password:             conf.OutputPassword,
		Database:             conf.OutputDb,
		AdditionalConnParams: additionalConnParams,
	}
}
