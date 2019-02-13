package cmd

import (
	"github.com/timescale/outflux/connections"
	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/schemamanagement/influx/influxqueries"
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
