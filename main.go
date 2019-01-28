package main

import (
	"fmt"
	"time"

	"github.com/timescale/outflux/extraction"

	extractionConfig "github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/ingestion"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

func main() {
	config := &extractionConfig.MeasureExtraction{
		Database:              "benchmark",
		Measure:               "cpu",
		ChunkSize:             10000,
		Limit:                 1000,
		DataChannelBufferSize: 1000,
	}

	config.DataChannelBufferSize = 1000
	connection := &clientutils.ConnectionParams{
		Server:   "http://localhost:8086",
		Username: "test",
		Password: "test",
	}

	extractor, _ := extraction.NewExtractor(config, connection)

	conParams := make(map[string]string)
	conParams["sslmode"] = "disable"
	ingestionConfig := &ingestionConfig.Config{
		Server:               "localhost:5432",
		Username:             "test",
		Password:             "test",
		SchemaStrategy:       ingestionConfig.DropAndCreate,
		Database:             "test",
		AdditionalConnParams: conParams,
		Schema:               "public",
	}
	start := time.Now()
	extractionInfo, _ := extractor.Start()
	ingestor := ingestion.NewIngestor(ingestionConfig, extractionInfo)
	ackChannel, err := ingestor.Start()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for ack := range ackChannel {
		fmt.Printf("Ack: %v\n", ack)
	}

	fmt.Printf("Ending in: %f seconds\n", time.Since(start).Seconds())
}
