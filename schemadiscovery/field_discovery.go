package schemadiscovery

import (
	"fmt"

	"github.com/influxdata/platform/chronograf/influx"
	"github.com/timescale/outflux/idrf"
)

// DiscoverMeasurementFields creates the ColumnInfo for the Fields of a given measurement
func DiscoverMeasurementFields(inluxClient *influx.Client, database, measurement string) ([]*idrf.ColumnInfo, error) {
	fields := fetchMeasurementFields(influxClient, database, measurment)
	if err != nil {
		return nil, err
	}

	return convertFields(fields)
}

func fetchMeasurementFields(influxClient *influx.Client, database, measurement string) ([][2]string, error) {
	showFieldsQuery := fmt.Sprintf(showFieldsQueryTemplate, measurement)
	values, err := ExecuteShowQuery(influxClient, database, showFieldsQuery, dontAcceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		errorString := fmt.Sprintf("field keys query returned unexpected result. "+
			"no values returned for measure '%s'", measurement)
		return nil, fmt.Errorf(errorString)
	}

	fieldKeys := make([][2]string, len(values))
	for index, valuesRow := range values {
		if len(valuesRow) != 2 {
			errorString := "Field key query returned unexpected result. " +
				"Field name and type not represented in two columns"
			return nil, fmt.Errorf(errorString)
		}

		fieldName := valuesRow[0].(string)
		fieldType := valuesRow[1].(string)
		fieldKeys[index] = [2]string{fieldName, fieldType}
	}

	return fieldKeys, nil
}

func convertFields(fieldsWithType [][2]string) ([]idrf.ColumnInfo, error) {
	columns := make([]idrf.ColumnInfo, len(fieldsWithType))
	for i, field := range fieldsWithType {
		columnType := convertDataType(field[1])
		idrfColumn, err := idrf.NewColumn(field[0], columnType)

		if err != nil {
			return nil, fmt.Errorf("Could not convert fields to IDRF. " + err.Error())
		}

		columns[i] = *idrfColumn
	}

	return columns, nil
}

func convertDataType(influxType string) idrf.DataType {
	switch influxType {
	case "float":
		return idrf.IDRFFloating
	case "string":
		return idrf.IDRFString
	case "integer":
		return idrf.IDRFInteger
	case "boolean":
		return idrf.IDRFBoolean
	default:
		panic("Unexpected value")
	}
}
