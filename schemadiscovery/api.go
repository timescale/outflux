package schemadiscovery

import (
	"fmt"

	"github.com/timescale/outflux/idrf"
)

const (
	// Flag to signify to executeShowQuery whether empty results are acceptable
	acceptEmptyResultFlag     = true
	dontAcceptEmptyResultFlag = false
	showMeasurementsQuery     = "SHOW MEASUREMENTS"
	showFieldsQueryTemplate   = "SHOW FIELD KEYS FROM %s"
	showTagsQueryTemplate     = "SHOW TAG KEYS FROM %s"
)

// InfluxDatabaseSchema will do something
func InfluxDatabaseSchema(connectionParams *ConnectionParams, database string) ([]*idrf.DataSetInfo, error) {

	return nil, nil
}

// InfluxMeasurementSchema extracts the IDRF schema definition for a specified measure of a InfluxDB database
func InfluxMeasurementSchema(connectionParams *ConnectionParams, database string, measure string) (*idrf.DataSetInfo, error) {
	influxClient, err := CreateInfluxClient(connectionParams)
	defer (influxClient).Close()

	if err != nil {
		return nil, err
	}

	measurements, err := FetchAvailableMeasurements(influxClient, database)
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

	idrfTags, err := DisoverMeasurementTags(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfFields, err := DiscoverMeasurementFields(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfTimeColumn, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	allColumns := []idrf.ColumnInfo{*idrfTimeColumn}
	allColumns = append(allColumns, idrfTags...)
	allColumns = append(allColumns, idrfFields...)
	dataSet, err := idrf.NewDataSet(measure, allColumns)
	return dataSet, err
}
