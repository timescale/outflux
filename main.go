package main

import (
	"fmt"
	"time"

	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/utils"

	extractionConfig "github.com/timescale/outflux/extraction/config"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

func main() {
	measureConfig := &extractionConfig.MeasureExtraction{
		Database:              "benchmark",
		Measure:               "cpu",
		ChunkSize:             10000,
		Limit:                 1000,
		DataChannelBufferSize: 1000,
	}

	connection := &clientutils.ConnectionParams{
		Server:   "http://localhost:8086",
		Username: "test",
		Password: "test",
	}

	extractionConfig := &extractionConfig.Config{
		ExtractorID:       "extractor 1",
		Connection:        connection,
		MeasureExtraction: measureConfig,
	}

	conParams := make(map[string]string)
	conParams["sslmode"] = "disable"
	ingestionConfig := &ingestionConfig.Config{
		IngestorID:           "ingestor 1",
		Server:               "localhost:5432",
		Username:             "test",
		Password:             "test",
		SchemaStrategy:       ingestionConfig.DropAndCreate,
		Database:             "test",
		AdditionalConnParams: conParams,
		Schema:               "public",
		BatchSize:            10000,
	}
	start := time.Now()

	errorBroadcaster := utils.NewErrorBroadcaster()
	config := &pipeline.OutfluxConfig{
		IngestionConfig:  ingestionConfig,
		ExtractionConfig: extractionConfig,
	}

	pipe := &pipeline.ExecutionPipeline{
		ID:               "pipe 1",
		Config:           config,
		ErrorBroadcaster: errorBroadcaster,
	}

	err := pipe.Start()
	if err != nil {
		fmt.Printf("Error in pipeline: %v\n", err)
	}
	fmt.Printf("Ended in: %f seconds\n", time.Since(start).Seconds())
}
