package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	extractionConfig "github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
)

func CreatePipelines(dataSets []*idrf.DataSetInfo, conf *MigrationConfig) []ExecutionPipeline {
	pipelines := make([]ExecutionPipeline, len(dataSets))
	for i, dataSet := range dataSets {
		pipelines[i] = &defaultExecutionPipeline{
			id:     fmt.Sprintf("pipe_%d", i),
			config: createPipelineConf(i, dataSet, conf),
		}
	}
	return pipelines
}

func createPipelineConf(pipeNum int, dataSet *idrf.DataSetInfo, conf *MigrationConfig) *PipelineConfig {
	ex := createExtractionConf(pipeNum, conf, dataSet)
	in := createIngestionConf(pipeNum, conf)
	return &PipelineConfig{
		ExtractionConfig: ex,
		IngestionConfig:  in,
	}
}

func createIngestionConf(pipeNum int, args *MigrationConfig) *ingestionConfig.Config {
	additionalConnParams := make(map[string]string)
	additionalConnParams["sslmode"] = args.OutputDbSslMode
	return &ingestionConfig.Config{
		IngestorID:              fmt.Sprintf("pipe_%d_ing", pipeNum),
		BatchSize:               args.ChunkSize,
		Server:                  args.OutputHost,
		Username:                args.OutputUser,
		Password:                args.OutputPassword,
		SchemaStrategy:          args.OutputSchemaStrategy,
		Database:                args.OutputDb,
		AdditionalConnParams:    additionalConnParams,
		Schema:                  args.OutputSchema,
		RollbackOnExternalError: args.RollbackAllMeasureExtractionsOnError,
	}
}

func createExtractionConf(pipeNum int, conf *MigrationConfig, dataSet *idrf.DataSetInfo) *extractionConfig.Config {
	measureExtractionConf := &extractionConfig.MeasureExtraction{
		Database:              conf.InputDb,
		Measure:               dataSet.DataSetName,
		From:                  conf.From,
		To:                    conf.To,
		ChunkSize:             conf.ChunkSize,
		Limit:                 conf.Limit,
		DataChannelBufferSize: conf.DataBuffer,
	}
	connection := &clientutils.ConnectionParams{
		Server:   conf.InputHost,
		Username: conf.InputUser,
		Password: conf.InputPass,
	}

	ex := &extractionConfig.Config{
		ExtractorID:       fmt.Sprintf("pipe_%d_ext", pipeNum),
		MeasureExtraction: measureExtractionConf,
		Connection:        connection,
		DataSet:           dataSet,
	}

	return ex
}
