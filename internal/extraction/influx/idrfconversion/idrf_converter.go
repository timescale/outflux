package idrfconversion

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/timescale/outflux/internal/idrf"
)

// IdrfConverter defines methods to convert the results of an InfluxDB Query result row to IDRF
type IdrfConverter interface {
	Convert(row []interface{}) (idrf.Row, error)
}

// NewIdrfConverter creates an instance of the IdrfConverter that converts the results
// of an InfluxDB Query result row to IDRF
func NewIdrfConverter(dataSet *idrf.DataSet) IdrfConverter {
	return &defaultIdrfConverter{dataSet}
}

type defaultIdrfConverter struct {
	dataSet *idrf.DataSet
}

func (conv *defaultIdrfConverter) Convert(row []interface{}) (idrf.Row, error) {
	if len(row) != len(conv.dataSet.Columns) {
		return nil, fmt.Errorf(
			"could not convert extracted row, number of extracted values is %d, expected %d values",
			len(row), len(conv.dataSet.Columns))
	}

	converted := make([]interface{}, len(row))
	for i, item := range row {
		converted[i] = convertByType(item, conv.dataSet.Columns[i].DataType)
	}

	return converted, nil
}

func convertByType(rawValue interface{}, expected idrf.DataType) interface{} {
	if rawValue == nil {
		return nil
	}

	switch {
	case expected == idrf.IDRFInteger32:
		switch v := rawValue.(type) {
		case json.Number:
			valAsInt64, _ := v.Int64()
			return int32(valAsInt64)
		case string:
			valAsInt64, _ := strconv.ParseInt(v, 10, 64)
			return int32(valAsInt64)
		}
		return nil
	case expected == idrf.IDRFInteger64:
		switch v := rawValue.(type) {
		case json.Number:
			valAsInt64, _ := v.Int64()
			return valAsInt64
		case string:
			valAsInt64, _ := strconv.ParseInt(v, 10, 64)
			return valAsInt64
		}
		return nil
	case expected == idrf.IDRFDouble:
		switch v := rawValue.(type) {
		case json.Number:
			valAsFloat64, _ := v.Float64()
			return valAsFloat64
		case string:
			valAsFloat64, _ := strconv.ParseFloat(v, 64)
			return valAsFloat64
		}
		return nil
	case expected == idrf.IDRFSingle:
		switch v := rawValue.(type) {
		case json.Number:
			valAsFloat64, _ := v.Float64()
			return float32(valAsFloat64)
		case string:
			valAsFloat64, _ := strconv.ParseFloat(v, 64)
			return float32(valAsFloat64)
		}
		return nil
	case expected == idrf.IDRFTimestamptz || expected == idrf.IDRFTimestamp:
		ts, _ := time.Parse(time.RFC3339, rawValue.(string))
		return ts
	default:
		return rawValue
	}
}
