package cmd

import (
	"context"
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

		err = migrate(migrateArgs)
		if err != nil {
			panic(err)
		}
	},
}

func migrate(args *pipeline.MigrationConfig) error {
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
		return err
	}

	log.Printf("Creating %d execution pipelines\n", len(influxMeasures))
	pipelines := pipeline.CreatePipelines(discoveredDataSets, args)

	pipelineSemaphore := semaphore.NewWeighted(int64(args.MaxParallel))
	ctx := context.Background()
	pipeChannel := make(chan bool, len(pipelines))

	// schedule all pipelines, as soon a value in the semaphore is available, execution will start
	for _, pipe := range pipelines {
		go pipeRoutine(ctx, pipelineSemaphore, pipe, pipeChannel)
	}

	log.Println("All pipelines scheduled")
	for range pipelines {
		<-pipeChannel
	}

	log.Println("All pipelines completed")

	executionTime := time.Since(startTime).Seconds()
	log.Printf("Execution time: %.3f seconds\n", executionTime)
	return nil
}

func pipeRoutine(ctx context.Context, semaphore *semaphore.Weighted, pipe pipeline.ExecutionPipeline,
	pipeChannel chan bool) {
	semaphore.Acquire(ctx, 1)
	log.Printf("%s starting execution\n", pipe.ID())
	err := pipe.Start()
	if err != nil {
		log.Printf("%s: %v\n", pipe.ID(), err)
	}
	defer semaphore.Release(1)
	pipeChannel <- true
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
