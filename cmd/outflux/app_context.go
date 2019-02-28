package main

import (
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type appContext struct {
	ics                connections.InfluxConnectionService
	tscs               connections.TSConnectionService
	pipeService        pipeline.PipeService
	influxQueryService influxqueries.InfluxQueryService
	extractorService   extraction.ExtractorService
}

func initAppContext() *appContext {
	tscs := connections.NewTSConnectionService()
	ics := connections.NewInfluxConnectionService()
	ingestorService := ingestion.NewIngestorService()
	influxQueryService := influxqueries.NewInfluxQueryService()
	extractorService := extraction.NewExtractorService(influxQueryService)
	pipeService := pipeline.NewPipeService(tscs, ics, ingestorService, extractorService)
	return &appContext{
		ics:                ics,
		tscs:               tscs,
		pipeService:        pipeService,
		influxQueryService: influxqueries.NewInfluxQueryService(),
		extractorService:   extractorService,
	}
}
