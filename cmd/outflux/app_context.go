package main

import (
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type appContext struct {
	ics                  connections.InfluxConnectionService
	tscs                 connections.TSConnectionService
	pipeService          pipeline.PipeService
	influxQueryService   influxqueries.InfluxQueryService
	extractorService     extraction.ExtractorService
	schemaManagerService schemamanagement.SchemaManagerService
}

func initAppContext() *appContext {
	tscs := connections.NewTSConnectionService()
	ics := connections.NewInfluxConnectionService()
	ingestorService := ingestion.NewIngestorService()
	influxQueryService := influxqueries.NewInfluxQueryService()
	schemaManagerService := schemamanagement.NewSchemaManagerService(influxQueryService)
	extractorService := extraction.NewExtractorService(schemaManagerService)
	pipeService := pipeline.NewPipeService(tscs, ics, ingestorService, extractorService)
	return &appContext{
		ics:                  ics,
		tscs:                 tscs,
		pipeService:          pipeService,
		influxQueryService:   influxQueryService,
		extractorService:     extractorService,
		schemaManagerService: schemaManagerService,
	}
}
