package discovery

import (
	"fmt"
	"log"
	"sort"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

const (
	showFieldsQueryTemplate = `SHOW FIELD KEYS FROM "%s"."%s"`
)

// FieldExplorer defines an API for discovering InfluxDB fields of a specified measurement
type FieldExplorer interface {
	// DiscoverMeasurementFields creates the ColumnInfo for the Fields of a given measurement
	DiscoverMeasurementFields(influxClient influx.Client, db, rp, measurement string, onConflictConvertIntToFloat bool) ([]*idrf.Column, error)
}

type defaultFieldExplorer struct {
	queryService influxqueries.InfluxQueryService
}

// NewFieldExplorer creates a new instance of the Field discovert API
func NewFieldExplorer(queryService influxqueries.InfluxQueryService) FieldExplorer {
	return &defaultFieldExplorer{queryService}
}

// InfluxDB can have different data types for the same field accross
// different shards. If a field is discovered with Int64 and Float64 type
// and the 'onConflictConvertIntToFloat' flag is TRUE it will allow the field to be converted to float,
// otherwise it will return an error
func (fe *defaultFieldExplorer) DiscoverMeasurementFields(influxClient influx.Client, db, rp, measurement string, onConflictConvertIntToFloat bool) ([]*idrf.Column, error) {
	fields, err := fe.fetchMeasurementFields(influxClient, db, rp, measurement)
	if err != nil {
		return nil, fmt.Errorf("error fetching fields for measurement '%s'\n%v", measurement, err)
	}

	return convertFields(fields, onConflictConvertIntToFloat)
}

func (fe *defaultFieldExplorer) fetchMeasurementFields(influxClient influx.Client, db, rp, measurement string) ([][2]string, error) {
	showFieldsQuery := fmt.Sprintf(showFieldsQueryTemplate, rp, measurement)
	result, err := fe.queryService.ExecuteShowQuery(influxClient, db, showFieldsQuery)

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

func convertFields(fieldsWithType [][2]string, convertInt64ToFloat64 bool) ([]*idrf.Column, error) {
	columnMap, err := chooseDataTypeForFields(fieldsWithType, convertInt64ToFloat64)
	if err != nil {
		return nil, err
	}

	columns := make([]*idrf.Column, len(columnMap))
	currentColumn := 0
	columnNames := make([]string, len(columnMap))
	for columnName := range columnMap {
		columnNames[currentColumn] = columnName
		currentColumn++
	}
	sort.Strings(columnNames)
	for i, columnName := range columnNames {
		columnType := columnMap[columnName]
		idrfColumn, err := idrf.NewColumn(columnName, columnType)

		if err != nil {
			return nil, fmt.Errorf("could not convert field to Intermediate Data Representation Format. \n%v", err.Error())
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

func chooseDataTypeForFields(fieldsWithType [][2]string, convertInt64ToFloat64 bool) (map[string]idrf.DataType, error) {
	columnMap := make(map[string]idrf.DataType)
	for _, field := range fieldsWithType {
		fieldName := field[0]
		fieldType := field[1]
		columnType := convertDataType(fieldType)
		existingType, ok := columnMap[fieldName]
		if !ok {
			columnMap[fieldName] = columnType
			continue
		} else if columnType.CanFitInto(existingType) {
			log.Printf("Field %s exists as %s and %s in the same measurement.", fieldName, existingType, columnType)
			log.Printf("Will be cast to %s during migration", existingType)
			continue
		} else if existingType.CanFitInto(columnType) {
			columnMap[fieldName] = columnType
			log.Printf("Field %s exists as %s and %s in the same measurement.", fieldName, existingType, columnType)
			log.Printf("Will be cast to %s during migration", columnType)
			continue
		} else if convertInt64ToFloat64 && intFloatCombo(existingType, columnType) {
			log.Printf("Field %s exists as %s and %s in the same measurement.", fieldName, existingType, columnType)
			log.Printf("Flag set to cast int64 to float64 for this field during migration")
			columnMap[fieldName] = idrf.IDRFDouble
			continue
		}

		return nil, fmt.Errorf("field '%s' has incomparable types accross multiple shards. "+
			"Exists with type %s and %s", fieldName, existingType, columnType)
	}

	return columnMap, nil
}

func intFloatCombo(oneType, secondType idrf.DataType) bool {
	return (oneType == idrf.IDRFInteger64 && secondType == idrf.IDRFDouble) ||
		(oneType == idrf.IDRFDouble && secondType == idrf.IDRFInteger64)
}
