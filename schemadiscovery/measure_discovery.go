package schemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/platform/chronograf/influx"
)

// FetchAvailableMeasurements returns the names of all available measurements for a given database,
// or an error if the query could not be executed, or the result was in an unexpected format
func FetchAvailableMeasurements(influxClient *influx.Client, database string) ([]string, error) {
	values, err := executeShowQuery(influxClient, database, showMeasurementsQuery, dontAcceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	measureNames := make([]string, len(values))
	for index, valuesRow := range values {
		if len(valuesRow) != 1 {
			errorString := "measurement discovery query returned unexpected result. " +
				"measurement names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		measureNames[index] = valuesRow[0].(string)
	}

	return measureNames, nil
}
