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
	config, _ := extractionConfig.NewMeasureExtractionConfig("benchmark", "cpu", 10000, 1000, "", "")
	config.DataChannelBufferSize = 1000
	connection := &clientutils.ConnectionParams{
		Server:   "http://localhost:8086",
		Username: "test",
		Password: "test",
	}

	extractor := extraction.NewExtractor(config, connection)

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
	ingestor := ingestion.NewIngestor(ingestionConfig)
	start := time.Now()
	extractionInfo, _ := extractor.Start()
	ackChannel, err := ingestor.Start(extractionInfo)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for ack := range ackChannel {
		fmt.Printf("Ack: %v\n", ack)
	}

	fmt.Printf("Ending in: %f seconds\n", time.Since(start).Seconds())
}
