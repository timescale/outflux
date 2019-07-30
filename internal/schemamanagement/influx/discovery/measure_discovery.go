package discovery

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

const (
	showMeasurementsQuery = "SHOW MEASUREMENTS"
)

// MeasureExplorer defines an API for discovering the available measures in an InfluxDB database
type MeasureExplorer interface {
	FetchAvailableMeasurements(influxClient influx.Client, db, rp string, onConflictConvertIntToFloat bool) ([]string, error)
}

// defaultMeasureExplorer contains the functions that can be swapped out during testing
type defaultMeasureExplorer struct {
	queryService  influxqueries.InfluxQueryService
	fieldExplorer FieldExplorer
}

// NewMeasureExplorer creates a new implementation of the MeasureExplorer API
func NewMeasureExplorer(queryService influxqueries.InfluxQueryService, fieldExplorer FieldExplorer) MeasureExplorer {
	return &defaultMeasureExplorer{
		queryService:  queryService,
		fieldExplorer: fieldExplorer,
	}
}

// FetchAvailableMeasurements returns the names of all available measurements for a given database,
// or an error if the query could not be executed, or the result was in an unexpected format
func (me *defaultMeasureExplorer) FetchAvailableMeasurements(influxClient influx.Client, db, rp string, onConflictConvertIntToFloat bool) ([]string, error) {
	result, err := me.queryService.ExecuteShowQuery(influxClient, db, showMeasurementsQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %s\nerror: %v", showMeasurementsQuery, err)
	}

	measuresInDb := make([]string, len(result.Values))
	for index, valuesRow := range result.Values {
		if len(valuesRow) != 1 {
			errorString := "measurement discovery query returned unexpected result. " +
				"measurement names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		measuresInDb[index] = valuesRow[0]
	}

	measuresInRP := []string{}
	for _, measure := range measuresInDb {
		_, err := me.fieldExplorer.DiscoverMeasurementFields(influxClient, db, rp, measure, onConflictConvertIntToFloat)
		if err != nil {
			log.Printf("Will ignore measurement '%s' because:\n%s", measure, err.Error())
			continue
		}

		measuresInRP = append(measuresInRP, measure)
	}
	return measuresInRP, nil
}
