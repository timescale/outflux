package main

import (
	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type appContext struct {
	ics                  connections.InfluxConnectionService
	tscs                 connections.TSConnectionService
	pipeService          cli.PipeService
	influxQueryService   influxqueries.InfluxQueryService
	extractorService     extraction.ExtractorService
	schemaManagerService schemamanagement.SchemaManagerService
	transformerService   cli.TransformerService
}

func initAppContext() *appContext {
	tscs := connections.NewTSConnectionService()
	ics := connections.NewInfluxConnectionService()
	ingestorService := ingestion.NewIngestorService()
	influxQueryService := influxqueries.NewInfluxQueryService()
	schemaManagerService := schemamanagement.NewSchemaManagerService(influxQueryService)
	extractorService := extraction.NewExtractorService(schemaManagerService)
	influxTagExplorer := discovery.NewTagExplorer(influxQueryService)
	influxFieldExplorer := discovery.NewFieldExplorer(influxQueryService)
	transformerService := cli.NewTransformerService(influxTagExplorer, influxFieldExplorer)
	pipeService := cli.NewPipeService(ingestorService, extractorService, transformerService)
	return &appContext{
		ics:                  ics,
		tscs:                 tscs,
		pipeService:          pipeService,
		influxQueryService:   influxQueryService,
		extractorService:     extractorService,
		schemaManagerService: schemaManagerService,
		transformerService:   transformerService,
	}
}
