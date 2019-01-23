package discovery

import (
	"fmt"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	influx "github.com/influxdata/influxdb/client/v2"
)

const (
	showMeasurementsQuery = "SHOW MEASUREMENTS"
)

// MeasureExplorer defines an API for discovering the available measures in an InfluxDB database
type MeasureExplorer interface {
	FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error)
}

// defaultMeasureExplorer contains the functions that can be swapped out during testing
type defaultMeasureExplorer struct {
	utils clientutils.ClientUtils
}

// NewMeasureExplorer creates a new implementation of the MeasureExplorer API
func NewMeasureExplorer() MeasureExplorer {
	return &defaultMeasureExplorer{
		utils: clientutils.NewUtils(),
	}
}

// FetchAvailableMeasurements returns the names of all available measurements for a given database,
// or an error if the query could not be executed, or the result was in an unexpected format
func (me *defaultMeasureExplorer) FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error) {
	result, err := me.utils.ExecuteShowQuery(influxClient, database, showMeasurementsQuery)

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
