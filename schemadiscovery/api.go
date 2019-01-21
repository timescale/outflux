package schemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	// Flag to signify to executeShowQuery whether empty results are acceptable
	acceptEmptyResultFlag     = true
	dontAcceptEmptyResultFlag = false
	showMeasurementsQuery     = "SHOW MEASUREMENTS"
	showFieldsQueryTemplate   = "SHOW FIELD KEYS FROM %s"
	showTagsQueryTemplate     = "SHOW TAG KEYS FROM %s"
)

type apiFn struct {
	createInfluxClient func(*clientutils.ConnectionParams) (influx.Client, error)
	fetchMeasurements  func(influx.Client, string) ([]string, error)
	discoverTags       func(influx.Client, string, string) ([]*idrf.ColumnInfo, error)
	discoverFields     func(influx.Client, string, string) ([]*idrf.ColumnInfo, error)
}

var (
	apiFunctions = apiFn{
		createInfluxClient: clientutils.CreateInfluxClient,
		fetchMeasurements:  FetchAvailableMeasurements,
		discoverFields:     DiscoverMeasurementFields,
		discoverTags:       DiscoverMeasurementTags,
	}
)

// InfluxDatabaseSchema extracts the IDRF schema definitions for all measures of a specified InfluxDB database
func InfluxDatabaseSchema(connectionParams *clientutils.ConnectionParams, database string) ([]*idrf.DataSetInfo, error) {
	influxClient, err := apiFunctions.createInfluxClient(connectionParams)

	if err != nil {
		return nil, err
	}

	defer influxClient.Close()

	measurements, err := apiFunctions.fetchMeasurements(influxClient, database)
	if err != nil {
		return nil, err
	}

	dataSets := make([]*idrf.DataSetInfo, len(measurements))
	for i, measure := range measurements {
		dataSets[i], err = constructDataSet(influxClient, database, measure)
		if err != nil {
			return nil, err
		}
	}

	return dataSets, nil
}

// InfluxMeasurementSchema extracts the IDRF schema definition for a specified measure of a InfluxDB database
func InfluxMeasurementSchema(connectionParams *clientutils.ConnectionParams, database, measure string) (*idrf.DataSetInfo, error) {
	influxClient, err := apiFunctions.createInfluxClient(connectionParams)

	if err != nil {
		return nil, err
	}

	defer influxClient.Close()

	measurements, err := apiFunctions.fetchMeasurements(influxClient, database)
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

	return constructDataSet(influxClient, database, measure)
}

func constructDataSet(influxClient influx.Client, database, measure string) (*idrf.DataSetInfo, error) {
	idrfTags, err := apiFunctions.discoverTags(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfFields, err := apiFunctions.discoverFields(influxClient, database, measure)
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
