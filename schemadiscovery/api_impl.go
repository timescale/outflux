package schemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
	"github.com/timescale/outflux/schemadiscovery/discovery"
)

// NewSchemaExplorer creates an instance implementing the schema discovery API
func NewSchemaExplorer() SchemaExplorer {
	return &defaultSchemaExplorer{
		&defaultInfluxDatabaseSchemaExplorer{},
		&defaultInfluxMeasurementSchemaExplorer{},
	}
}

// NewSchemaExplorerWith creates an instance implementing the schema discovery API with provided dependencies
func NewSchemaExplorerWith(
	dbExplorer influxDatabaseSchemaExplorer,
	measureExplorer influxMeasurementSchemaExplorer,
) SchemaExplorer {
	return &defaultSchemaExplorer{
		dbExplorer, measureExplorer,
	}
}

type defaultSchemaExplorer struct {
	influxDatabaseSchemaExplorer
	influxMeasurementSchemaExplorer
}

type defaultInfluxDatabaseSchemaExplorer struct {
	clientUtils     clientutils.ClientUtils
	measureExplorer discovery.MeasureExplorer
	tagExplorer     discovery.TagExplorer
	fieldExplorer   discovery.FieldExplorer
}

// InfluxDatabaseSchema extracts the IDRF schema definitions for all measures of a specified InfluxDB database
func (se *defaultInfluxDatabaseSchemaExplorer) InfluxDatabaseSchema(connectionParams *clientutils.ConnectionParams, database string) ([]*idrf.DataSetInfo, error) {
	influxClient, err := se.clientUtils.CreateInfluxClient(connectionParams)

	if err != nil {
		return nil, err
	}

	defer influxClient.Close()

	measurements, err := se.measureExplorer.FetchAvailableMeasurements(influxClient, database)
	if err != nil {
		return nil, err
	}

	dataSets := make([]*idrf.DataSetInfo, len(measurements))
	for i, measure := range measurements {
		dataSets[i], err = constructDataSet(se.tagExplorer, se.fieldExplorer, influxClient, database, measure)
		if err != nil {
			return nil, err
		}
	}

	return dataSets, nil
}

type defaultInfluxMeasurementSchemaExplorer struct {
	clientUtils     clientutils.ClientUtils
	measureExplorer discovery.MeasureExplorer
	tagExplorer     discovery.TagExplorer
	fieldExplorer   discovery.FieldExplorer
}

// InfluxMeasurementSchema extracts the IDRF schema definition for a specified measure of a InfluxDB database
func (se *defaultInfluxMeasurementSchemaExplorer) InfluxMeasurementSchema(
	connectionParams *clientutils.ConnectionParams,
	database, measure string,
) (*idrf.DataSetInfo, error) {
	influxClient, err := se.clientUtils.CreateInfluxClient(connectionParams)

	if err != nil {
		return nil, err
	}

	defer influxClient.Close()

	measurements, err := se.measureExplorer.FetchAvailableMeasurements(influxClient, database)
	if err != nil {
		return nil, err
	}

	measureMissing := true
	for _, returnedMeasure := range measurements {
		if returnedMeasure == measure {
			measureMissing = false
			break
		}
	}

	if measureMissing {
		return nil, fmt.Errorf("measure '%s' not found in database '%s'", measure, database)
	}

	return constructDataSet(se.tagExplorer, se.fieldExplorer, influxClient, database, measure)
}

func constructDataSet(
	tagExplorer discovery.TagExplorer,
	fieldExplorer discovery.FieldExplorer,
	influxClient influx.Client,
	database, measure string,
) (*idrf.DataSetInfo, error) {
	idrfTags, err := tagExplorer.DiscoverMeasurementTags(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfFields, err := fieldExplorer.DiscoverMeasurementFields(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfTimeColumn, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	allColumns := []*idrf.ColumnInfo{idrfTimeColumn}
	allColumns = append(allColumns, idrfTags...)
	allColumns = append(allColumns, idrfFields...)
	dataSet, err := idrf.NewDataSet(measure, allColumns)
	return dataSet, err
}
