package schemadiscovery

import (
	"fmt"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	influx "github.com/influxdata/influxdb/client/v2"
)

// MeasureDiscoveryFns contains the functions that can be swapped out during testing
type MeasureDiscoveryFns struct {
	executeShowQuery func(*influx.Client, string, string) (*clientutils.InfluxShowResult, error)
}

var (
	mdFunctions = MeasureDiscoveryFns{
		executeShowQuery: clientutils.ExecuteShowQuery,
	}
)

// FetchAvailableMeasurements returns the names of all available measurements for a given database,
// or an error if the query could not be executed, or the result was in an unexpected format
func FetchAvailableMeasurements(influxClient *influx.Client, database string) ([]string, error) {
	result, err := mdFunctions.executeShowQuery(influxClient, database, showMeasurementsQuery)

	if err != nil {
		return nil, err
	}

	measureNames := make([]string, len(result.Values))
	for index, valuesRow := range result.Values {
		if len(valuesRow) != 1 {
			errorString := "measurement discovery query returned unexpected result. " +
				"measurement names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		measureNames[index] = valuesRow[0]
	}

	return measureNames, nil
}
