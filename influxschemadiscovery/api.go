package influxschemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
)

const (
	// Flag to signify to executeShowQuery whether empty results are acceptable
	acceptEmptyResultFlag     = true
	dontAcceptEmptyResultFlag = false
)

// InfluxDatabaseSchema will do something
func InfluxDatabaseSchema(connectionParams *ConnectionParams, database string) ([]*idrf.DataSetInfo, error) {

	return nil, nil
}

// InfluxMeasurementSchema extracts the IDRF schema definition for a specified measure of a InfluxDB database
func InfluxMeasurementSchema(connectionParams *ConnectionParams, database string, measure string) (*idrf.DataSetInfo, error) {
	influxClient, err := CreateInfluxClient(connectionParams)
	defer (*influxClient).Close()

	if err != nil {
		return nil, err
	}

	measurements, err := fetchAvailableMeasurements(influxClient, database)
	if err != nil {
		return nil, err
	}

	measureMissing := true
	for _, returnedMeasure := range *measurements {
		if returnedMeasure == measure {
			measureMissing = false
			break
		}
	}

	if measureMissing {
		return nil, fmt.Errorf("Measure <%s> not found in database <%s>", measure, database)
	}

	fields, err := fetchMeasurementFields(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	tags, err := fetchMeasurementTags(influxClient, database, measure)
	if err != nil {
		return nil, err
	}

	idrfFields, err := convertFields(fields)
	if err != nil {
		return nil, err
	}

	idrfTags, err := convertTags(tags)

	if err != nil {
		return nil, err
	}

	idrfTimeColumn, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	allColumns := []idrf.ColumnInfo{*idrfTimeColumn}
	allColumns = append(allColumns, *idrfTags...)
	allColumns = append(allColumns, *idrfFields...)
	dataSet, err := idrf.NewDataSet(measure, allColumns)
	return dataSet, err
}

func fetchAvailableMeasurements(influxClient *influx.Client, database string) (*[]string, error) {
	showMeasurementsQuery := "SHOW MEASUREMENTS"
	values, err := executeShowQuery(influxClient, database, showMeasurementsQuery, dontAcceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	if len(*values) == 0 {
		errorString := "Measurement discovery query returned unexpected result. " +
			"No values returned for measure names"
		return nil, fmt.Errorf(errorString)
	}

	measureNames := make([]string, len(*values))
	for index, valuesRow := range *values {
		if len(valuesRow) != 1 {
			errorString := "Measurement discovery query returned unexpected result. " +
				"Measurement names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		measureNames[index] = valuesRow[0].(string)
	}

	return &measureNames, nil
}

func fetchMeasurementFields(influxClient *influx.Client, database string, measurement string) (*[][2]string, error) {
	showFieldsQuery := "SHOW FIELD KEYS FROM " + measurement
	values, err := executeShowQuery(influxClient, database, showFieldsQuery, dontAcceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	if len(*values) == 0 {
		errorString := fmt.Sprintf("Field keys query returned unexpected result. "+
			"No values returned for measure <%s>", measurement)
		return nil, fmt.Errorf(errorString)
	}

	fieldKeys := make([][2]string, len(*values))
	for index, valuesRow := range *values {
		if len(valuesRow) != 2 {
			errorString := "Field key query returned unexpected result. " +
				"Field name and type not represented in two columns"
			return nil, fmt.Errorf(errorString)
		}

		fieldName := valuesRow[0].(string)
		fieldType := valuesRow[1].(string)
		fieldKeys[index] = [2]string{fieldName, fieldType}
	}

	return &fieldKeys, nil
}

func fetchMeasurementTags(influxClient *influx.Client, database string, measure string) (*[]string, error) {
	showTagsQuery := "SHOW TAG KEYS FROM " + measure
	values, err := executeShowQuery(influxClient, database, showTagsQuery, acceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	if len(*values) == 0 {
		return &[]string{}, nil
	}

	tagNames := make([]string, len(*values))
	for index, valuesRow := range *values {
		if len(valuesRow) != 1 {
			errorString := "Tag discovery query returned unexpected result. " +
				"Tag names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		tagNames[index] = valuesRow[0].(string)
	}

	return &tagNames, nil
}

func executeShowQuery(influxClient *influx.Client, database string, query string, acceptEmpty bool) (*[][]interface{}, error) {
	resultPtr, err := ExecuteInfluxQuery(influxClient, database, query)
	if err != nil {
		return nil, err
	}

	result := *resultPtr
	if len(result) != 1 {
		errorString := "SHOW query failed. No results returned."
		return nil, fmt.Errorf(errorString)
	}

	series := result[0].Series
	if len(series) == 0 && acceptEmpty {
		// Empty result is acceptable
		return &[][]interface{}{}, nil
	} else if len(series) == 0 && !acceptEmpty {
		errorString := "SHOW query returned unexpected results. No series found when expecting one."
		return nil, fmt.Errorf(errorString)
	} else if len(series) > 1 {
		errorString := "SHOW query returned unexpected results. More than one series found."
		return nil, fmt.Errorf(errorString)
	}

	return &series[0].Values, nil
}

func convertFields(fieldsWithType *[][2]string) (*[]idrf.ColumnInfo, error) {
	columns := make([]idrf.ColumnInfo, len(*fieldsWithType))
	for i, field := range *fieldsWithType {
		columnType := convertDataType(field[1])
		idrfColumn, err := idrf.NewColumn(field[0], columnType)

		if err != nil {
			return nil, fmt.Errorf("Could not convert fields to IDRF. " + err.Error())
		}

		columns[i] = *idrfColumn
	}

	return &columns, nil
}

func convertTags(tags *[]string) (*[]idrf.ColumnInfo, error) {
	columns := make([]idrf.ColumnInfo, len(*tags))
	for i, tag := range *tags {
		idrfColumn, err := idrf.NewColumn(tag, idrf.IDRFString)

		if err != nil {
			return nil, fmt.Errorf("Could not convert tags to IDRF. " + err.Error())
		}

		columns[i] = *idrfColumn
	}

	return &columns, nil
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
