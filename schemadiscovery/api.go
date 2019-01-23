package schemadiscovery

import (
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

type influxMeasurementSchemaExplorer interface {
	InfluxMeasurementSchema(
		connectionParams *clientutils.ConnectionParams,
		database, measure string,
	) (*idrf.DataSetInfo, error)
}

type influxDatabaseSchemaExplorer interface {
	InfluxDatabaseSchema(connectionParams *clientutils.ConnectionParams, database string) ([]*idrf.DataSetInfo, error)
}

// SchemaExplorer defines the methods for discovering schemas of an influx database
type SchemaExplorer interface {
	influxMeasurementSchemaExplorer
	influxDatabaseSchemaExplorer
}
