package discovery

import (
	"fmt"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

const (
	showFieldsQueryTemplate       = "SHOW FIELD KEYS FROM \"%s\""
	showFieldsQueryWithRPTemplate = "SHOW FIELD KEYS FROM \"%s\".\"%s\""
)

// FieldExplorer defines an API for discoering InfluxDB fields of a specified measurement
type FieldExplorer interface {
	// DiscoverMeasurementFields creates the ColumnInfo for the Fields of a given measurement
	DiscoverMeasurementFields(influxClient influx.Client, database, measurement string) ([]*idrf.Column, error)
}

type defaultFieldExplorer struct {
	queryService influxqueries.InfluxQueryService
}

// NewFieldExplorer creates a new instance of the Field discovert API
func NewFieldExplorer(queryService influxqueries.InfluxQueryService) FieldExplorer {
	return &defaultFieldExplorer{queryService}
}

func (fe *defaultFieldExplorer) DiscoverMeasurementFields(influxClient influx.Client, database, measurement string) ([]*idrf.Column, error) {
	fields, err := fe.fetchMeasurementFields(influxClient, database, measurement)
	if err != nil {
		return nil, fmt.Errorf("error fetching fields for measurement '%s'\n%v", measurement, err)
	}

	return convertFields(fields)
}

func (fe *defaultFieldExplorer) fetchMeasurementFields(influxClient influx.Client, database, measurement string) ([][2]string, error) {
	var showFieldsQuery string
	if strings.Contains(measurement, ".") {
		parts := strings.SplitN(measurement, ".", 2)
		showFieldsQuery = fmt.Sprintf(showFieldsQueryWithRPTemplate, parts[0], parts[1])
	} else {
		showFieldsQuery = fmt.Sprintf(showFieldsQueryTemplate, measurement)
	}
	result, err := fe.queryService.ExecuteShowQuery(influxClient, database, showFieldsQuery)

	if err != nil {
		return nil, fmt.Errorf("error executing query: %s\n%v", showFieldsQuery, err)
	}

	if len(result.Values) == 0 {
		errorString := fmt.Sprintf("field keys query returned unexpected result. "+
			"no values returned for measure '%s'", measurement)
		return nil, fmt.Errorf(errorString)
	}

	fieldKeys := make([][2]string, len(result.Values))
	for index, valuesRow := range result.Values {
		if len(valuesRow) != 2 {
			errorString := "field key query returned unexpected result. " +
				"field name and type not represented in two columns"
			return nil, fmt.Errorf(errorString)
		}

		fieldName := valuesRow[0]
		fieldType := valuesRow[1]
		fieldKeys[index] = [2]string{fieldName, fieldType}
	}

	return fieldKeys, nil
}

func convertFields(fieldsWithType [][2]string) ([]*idrf.Column, error) {
	columns := make([]*idrf.Column, len(fieldsWithType))
	for i, field := range fieldsWithType {
		columnType := convertDataType(field[1])
		idrfColumn, err := idrf.NewColumn(field[0], columnType)

		if err != nil {
			return nil, fmt.Errorf("could not convert fields to IDRF. \n%v", err.Error())
		}

		columns[i] = idrfColumn
	}

	return columns, nil
}

func convertDataType(influxType string) idrf.DataType {
	switch influxType {
	case "float":
		return idrf.IDRFDouble
	case "string":
		return idrf.IDRFString
	case "integer":
		return idrf.IDRFInteger64
	case "boolean":
		return idrf.IDRFBoolean
	default:
		panic("Unexpected value")
	}
}
