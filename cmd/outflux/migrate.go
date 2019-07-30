package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/cli/flagparsers"
	"github.com/timescale/outflux/internal/connections"
	"golang.org/x/sync/semaphore"
)

func initMigrateCmd() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate database [measure1 measure2 ...]",
		Short: "Migrate the schema and data from InfluxDB measurements into TimescaleDB hypertables",
		Long: "Migrate the data from InfluxDB measurements into TimescaleDB. Schema discovery detects the required" +
			" table definition to be present in the target TimescaleDB and prepares it according to the selected startegy." +
			" Then the data is transferred, each measurement in a separate hyper-table",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			connArgs, migrateArgs, err := flagparsers.FlagsToMigrateConfig(cmd.Flags(), args)
			if err != nil {
				log.Fatal(err)
				return
			}

			err = migrate(app, connArgs, migrateArgs)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	flagparsers.AddConnectionFlagsToCmd(migrateCmd)
	migrateCmd.PersistentFlags().String(flagparsers.RetentionPolicyFlag, flagparsers.DefaultRetentionPolicy, "The retention policy to select the data from")
	migrateCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	migrateCmd.PersistentFlags().String(flagparsers.FromFlag, "", "If specified will export data with a timestamp >= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().String(flagparsers.ToFlag, "", "If specified will export data with a timestamp <= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().Uint64(flagparsers.LimitFlag, flagparsers.DefaultLimit, "If specified will limit the export points to it's value. 0 = NO LIMIT")
	migrateCmd.PersistentFlags().Uint16(flagparsers.ChunkSizeFlag, flagparsers.DefaultChunkSize, "The export query will request the data in chunks of this size. Must be > 0")
	migrateCmd.PersistentFlags().Uint16(flagparsers.DataBufferFlag, flagparsers.DefaultDataBufferSize, "Size of the buffer holding exported data ready to be inserted in the output database")
	migrateCmd.PersistentFlags().Uint8(flagparsers.MaxParallelFlag, flagparsers.DefaultMaxParallel, "Number of parallel measure extractions. One InfluxDB measure is exported using 1 worker")
	migrateCmd.PersistentFlags().Bool(flagparsers.RollbackOnExternalErrorFlag, flagparsers.DefaultRollbackOnExternalError, "If this flag is set, when an error occurs while extracting the data, the insertion will be rollbacked. Otherwise it will try to commit")
	migrateCmd.PersistentFlags().String(flagparsers.CommitStrategyFlag, flagparsers.DefaultCommitStrategy.String(), "Determines whether to commit on each chunk extracted from Influx, or at the end. Valid options: CommitOnEnd and CommitOnEachBatch")
	migrateCmd.PersistentFlags().Uint16(flagparsers.BatchSizeFlag, flagparsers.DefaultBatchSize, "The size of the batch inserted in to the output database")
	migrateCmd.PersistentFlags().Bool(flagparsers.TagsAsJSONFlag, flagparsers.DefaultTagsAsJSON, "If this flag is set to true, then the Tags of the influx measures being exported will be combined into a single JSONb column in Timescale")
	migrateCmd.PersistentFlags().String(flagparsers.TagsColumnFlag, flagparsers.DefaultTagsColumn, "When "+flagparsers.TagsAsJSONFlag+" is set, this column specifies the name of the JSON column for the tags")
	migrateCmd.PersistentFlags().Bool(flagparsers.FieldsAsJSONFlag, flagparsers.DefaultFieldsAsJSON, "If this flag is set to true, then the Fields of the influx measures being exported will be combined into a single JSONb column in Timescale")
	migrateCmd.PersistentFlags().String(flagparsers.FieldsColumnFlag, flagparsers.DefaultFieldsColumn, "When "+flagparsers.FieldsAsJSONFlag+" is set, this column specifies the name of the JSON column for the fields")
	migrateCmd.PersistentFlags().String(flagparsers.OutputSchemaFlag, flagparsers.DefaultOutputSchema, "The schema of the output database that the data will be inserted into")
	migrateCmd.PersistentFlags().Bool(flagparsers.MultishardIntFloatCast, flagparsers.DefaultMultishardIntFloatCast, "If a field is Int64 in one shard, and Float64 in another, with this flag it will be cast to Float64 despite possible data loss")
	return migrateCmd
}

func migrate(app *appContext, connArgs *cli.ConnectionConfig, args *cli.MigrationConfig) error {
	if args.Quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	if len(connArgs.InputMeasures) == 0 {
		influxConn, err := app.ics.NewConnection(influxConnParams(connArgs))
		if err != nil {
			return fmt.Errorf("could not open connection to Influx Server\n%v", err)
		}
		connArgs.InputMeasures, err = discoverMeasures(app, influxConn, connArgs.InputDb, args.RetentionPolicy, args.OnConflictConvertIntToFloat)
		influxConn.Close()
		if err != nil {
			return fmt.Errorf("could not discover the available measures for the input db '%s'", connArgs.InputDb)
		}
	}

	startTime := time.Now()
	pipelineSemaphore := semaphore.NewWeighted(int64(args.MaxParallel))
	ctx := context.Background()
	pipeChannels := makePipeChannels(len(connArgs.InputMeasures))

	// schedule all pipelines, as soon a value in the semaphore is available, execution will start
	for i, measure := range connArgs.InputMeasures {
		go pipeRoutine(ctx, pipelineSemaphore, app, connArgs, args, measure, pipeChannels[i])
	}

	log.Println("All pipelines scheduled")
	hasError := false
	pipeErrors := make([]error, len(pipeChannels))
	for i, pipeChannel := range pipeChannels {
		pipeErrors[i] = <-pipeChannel
		if pipeErrors[i] != nil {
			hasError = true
		}
	}

	log.Println("All pipelines finished")

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Migration execution time: %.3f seconds\n", executionTime)
	if hasError {
		return preparePipeErrors(pipeErrors)
	}

	return nil
}

func pipeRoutine(
	ctx context.Context,
	semaphore *semaphore.Weighted,
	app *appContext,
	connArgs *cli.ConnectionConfig,
	args *cli.MigrationConfig,
	measure string,
	pipeChannel chan error) {
	_ = semaphore.Acquire(ctx, 1)

	infConn, pgConn, err := openConnections(app, connArgs)

	if err != nil {
		pipeChannel <- fmt.Errorf("could not open connections to input and output database\n%v", err)
		return
	}
	defer infConn.Close()
	defer pgConn.Close()
	pipe, err := app.pipeService.Create(infConn, pgConn, measure, connArgs.InputDb, args)
	if err != nil {
		pipeChannel <- fmt.Errorf("could not create execution pipeline for measure '%s'\n%v", measure, err)
		return
	}

	log.Printf("%s starting execution\n", pipe.ID())
	err = pipe.Run()
	if err != nil {
		log.Printf("%s: %v\n", pipe.ID(), err)
		pipeChannel <- err
	}

	close(pipeChannel)
	semaphore.Release(1)
}

func makePipeChannels(numChannels int) []chan error {
	channels := make([]chan error, numChannels)
	for i := 0; i < numChannels; i++ {
		channels[i] = make(chan error)
	}

	return channels
}

func preparePipeErrors(errors []error) error {
	errString := "Migration finished with errors:\n"
	for _, err := range errors {
		if err != nil {
			errString += err.Error() + "\n"
		}
	}

	return fmt.Errorf(errString)
}

func openConnections(app *appContext, connArgs *cli.ConnectionConfig) (influx.Client, *pgx.Conn, error) {
	influxConn, err := app.ics.NewConnection(influxConnParams(connArgs))
	if err != nil {
		return nil, nil, fmt.Errorf("could not open connection to Influx Server\n%v", err)
	}

	tsConn, err := app.tscs.NewConnection(connArgs.OutputDbConnString)
	if err != nil {
		influxConn.Close()
		return nil, nil, fmt.Errorf("could not open connection to TimescaleDB Server\n%v", err)
	}

	return influxConn, tsConn, nil
}

func influxConnParams(connParams *cli.ConnectionConfig) *connections.InfluxConnectionParams {
	return &connections.InfluxConnectionParams{
		Server:      connParams.InputHost,
		Database:    connParams.InputDb,
		Username:    connParams.InputUser,
		Password:    connParams.InputPass,
		UnsafeHTTPS: connParams.InputUnsafeHTTPS,
	}
}
