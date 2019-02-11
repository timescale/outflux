package cmd

import (
	"github.com/timescale/outflux/connections"
	"github.com/timescale/outflux/pipeline"
	"github.com/timescale/outflux/schemamanagement/influx/influxqueries"
)

type appContext struct {
	influxConnectionService connections.InfluxConnectionService
	tsConnectionService     connections.TSConnectionService
	pipelinesService        pipeline.PipelinesService
	influxQueryService      influxqueries.InfluxQueryService
}

func initAppContext() *appContext {
	tsConnService := connections.NewTSConnectionService()
	influxConnService := connections.NewInfluxConnectionService()
	return &appContext{
		influxConnectionService: influxConnService,
		tsConnectionService:     tsConnService,
		pipelinesService:        pipeline.NewPipelinesService(tsConnService, influxConnService),
		influxQueryService:      influxqueries.NewInfluxQueryService(),
	}
}
