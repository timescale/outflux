package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
	"golang.org/x/sync/semaphore"

	"github.com/timescale/outflux/idrf"

	"github.com/spf13/cobra"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/schemadiscovery"
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
	defaultDataBufferSize          = 15000
	defaultChunkSize               = 15000
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

		errors := migrate(migrateArgs)
		if errors != nil {
			err = preparePipeErrors(errors)
			panic(err)
		}
	},
}

func migrate(args *pipeline.MigrationConfig) []error {
	startTime := time.Now()
	if args.Quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	influxDb := args.InputDb
	log.Printf("Selected input database: %s\n", influxDb)
	influxMeasures := args.InputMeasures

	var discoveredDataSets []*idrf.DataSetInfo
	var err error
	if len(influxMeasures) == 0 {
		discoveredDataSets, err = discoverSchemaForDatabase(args)
	} else {
		discoveredDataSets, err = discoverSchemaForMeasures(args)
	}

	if err != nil {
		toReturn := make([]error, 1)
		toReturn[0] = err
		return toReturn
	}

	log.Printf("Creating %d execution pipelines\n", len(influxMeasures))
	pipelines := pipeline.CreatePipelines(discoveredDataSets, args)

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
	log.Printf("Execution time: %.3f seconds\n", executionTime)
	if hasError {
		return pipeErrors
	}

	return nil
}

func pipeRoutine(ctx context.Context, semaphore *semaphore.Weighted, pipe pipeline.ExecutionPipeline,
	pipeChannel chan error) {
	defer close(pipeChannel)

	semaphore.Acquire(ctx, 1)
	defer semaphore.Release(1)

	log.Printf("%s starting execution\n", pipe.ID())
	err := pipe.Start()
	if err != nil {
		log.Printf("%s: %v\n", pipe.ID(), err)
		pipeChannel <- err
	}
}
func discoverSchemaForDatabase(args *pipeline.MigrationConfig) ([]*idrf.DataSetInfo, error) {
	log.Println("All measurements selected for exporting")
	schemaExplorer := schemadiscovery.NewSchemaExplorer()
	influxConnectionParams := &clientutils.ConnectionParams{
		Server:   args.InputHost,
		Username: args.InputUser,
		Password: args.InputPass,
	}
	discoveredDataSets, err := schemaExplorer.InfluxDatabaseSchema(influxConnectionParams, args.InputDb)
	if err != nil {
		log.Println("Couldn't discover the database schema")
		return nil, err
	}

	return discoveredDataSets, nil
}

func discoverSchemaForMeasures(args *pipeline.MigrationConfig) ([]*idrf.DataSetInfo, error) {
	log.Printf("Selected measurements for exporting: %v\n", args.InputMeasures)
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
			log.Printf("Could not discover schema for measurement: %s\n", measureName)
			return nil, err
		}
	}

	return discoveredDataSets, nil
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
