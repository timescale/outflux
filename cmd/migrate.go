package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/flagparsers"
	"github.com/timescale/outflux/pipeline"
	"golang.org/x/sync/semaphore"
)

func initMigrateCmd() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:  "migrate database [measure1 measure2 ...]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			app := initAppContext()
			migrateArgs, err := flagparsers.FlagsToMigrateConfig(cmd.Flags(), args)
			if err != nil {
				panic(err)
			}

			errors := migrate(app, migrateArgs)
			if errors != nil {
				err = preparePipeErrors(errors)
				panic(err)
			}
		},
	}
	migrateCmd.PersistentFlags().String(flagparsers.InputHostFlag, flagparsers.DefaultInputHost, "Host of the input database, http(s)://location:port.")
	migrateCmd.PersistentFlags().String(flagparsers.InputUserFlag, flagparsers.DefaultInputUser, "Username to use when connecting to the input database")
	migrateCmd.PersistentFlags().String(flagparsers.InputPassFlag, flagparsers.DefaultInputPass, "Password to use when connecting to the input database")
	migrateCmd.PersistentFlags().String(flagparsers.OutputHostFlag, flagparsers.DefaultOutputHost, "Host of the output database, location:port")
	migrateCmd.PersistentFlags().String(flagparsers.OutputUserFlag, "", "Username to use when connecting to the output database")
	migrateCmd.PersistentFlags().String(flagparsers.OutputPasswordFlag, "", "Password to use when connecting to the output database")
	migrateCmd.PersistentFlags().String(flagparsers.OutputDbFlag, "", "Output (Target) database that the data will be inserted into")
	migrateCmd.PersistentFlags().String(flagparsers.OutputSchemaFlag, "public", "The schema of the output database that the data will be inserted into")
	migrateCmd.PersistentFlags().String(flagparsers.OutputDbSslModeFlag, "disable", "SSL mode to use when connecting to the output server. Valid: disable, require, verify-ca, verify-full")
	migrateCmd.PersistentFlags().String(flagparsers.SchemaStrategyFlag, flagparsers.DefaultSchemaStrategy.String(), "Strategy to use for preparing the schema of the output database. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	migrateCmd.PersistentFlags().String(flagparsers.FromFlag, "", "If specified will export data with a timestamp >= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().String(flagparsers.ToFlag, "", "If specified will export data with a timestamp <= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().Uint64(flagparsers.LimitFlag, flagparsers.DefaultLimit, "If specified will limit the export points to it's value. 0 = NO LIMIT")
	migrateCmd.PersistentFlags().Uint16(flagparsers.ChunkSizeFlag, flagparsers.DefaultChunkSize, "The export query will request the data in chunks of this size. Must be > 0")
	migrateCmd.PersistentFlags().Uint16(flagparsers.DataBufferFlag, flagparsers.DefaultDataBufferSize, "Size of the buffer holding exported data ready to be inserted in the output database")
	migrateCmd.PersistentFlags().Uint8(flagparsers.MaxParallelFlag, flagparsers.DefaultMaxParallel, "Number of parallel measure extractions. One InfluxDB measure is exported using 1 worker")
	migrateCmd.PersistentFlags().Bool(flagparsers.RollbackOnExternalErrorFlag, flagparsers.DefaultRollbackOnExternalError, "If this flag is set, when an error occurs while extracting the data, the insertion will be rollbacked. Otherwise it will try to commit")
	return migrateCmd
}

func migrate(app *appContext, args *pipeline.MigrationConfig) []error {
	if args.Quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	discoveredDataSets, err := transferSchema(app, args.ToSchemaTransferConfig())

	if err != nil {
		return []error{err}
	}

	startTime := time.Now()
	log.Printf("Creating %d execution pipelines\n", len(discoveredDataSets))
	pipelines, err := app.ps.CreatePipelines(discoveredDataSets, args)
	if err != nil {
		return []error{err}
	}

	pipelineSemaphore := semaphore.NewWeighted(int64(args.MaxParallel))
	ctx := context.Background()
	pipeChannels := makePipeChannels(len(pipelines))

	// schedule all pipelines, as soon a value in the semaphore is available, execution will start
	for i, pipe := range pipelines {
		go pipeRoutine(ctx, pipelineSemaphore, pipe, pipeChannels[i])
	}

	log.Println("All pipelines scheduled")
	hasError := false
	pipeErrors := make([]error, len(pipelines))
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
		return pipeErrors
	}

	return nil
}

func pipeRoutine(ctx context.Context, semaphore *semaphore.Weighted, pipe pipeline.ExecutionPipeline,
	pipeChannel chan error) {
	semaphore.Acquire(ctx, 1)

	log.Printf("%s starting execution\n", pipe.ID())
	err := pipe.Start()
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
	errString := `
---------------------------------------------
Migration finished with errors:
`
	for _, err := range errors {
		errString += err.Error() + "\n"
	}

	return fmt.Errorf(errString)
}
