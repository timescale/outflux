package main

import (
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type appContext struct {
	ics  connections.InfluxConnectionService
	tscs connections.TSConnectionService
	ps   pipeline.PipelinesService
	iqs  influxqueries.InfluxQueryService
}

func initAppContext() *appContext {
	tscs := connections.NewTSConnectionService()
	ics := connections.NewInfluxConnectionService()
	return &appContext{
		ics:  ics,
		tscs: tscs,
		ps:   pipeline.NewPipelinesService(tscs, ics),
		iqs:  influxqueries.NewInfluxQueryService(),
	}
}
