package ingestion

import (
	"fmt"

	"github.com/timescale/outflux/idrf"
)

type IdrfConverterGenerator interface {
	Generate(dataSet *idrf.DataSetInfo) IdrfConverter
}

type defaultIdrfConverterGenerator struct{}

func NewIdrfConverterGenerator() IdrfConverterGenerator {
	return &defaultIdrfConverterGenerator{}
}

func (gen *defaultIdrfConverterGenerator) Generate(dataSet *idrf.DataSetInfo) IdrfConverter {
	return &defaultIdrfConverter{dataSet}
}

type IdrfConverter interface {
	ConvertValues(row idrf.Row) ([]interface{}, error)
}

type defaultIdrfConverter struct {
	dataSet *idrf.DataSetInfo
}

func (conv *defaultIdrfConverter) ConvertValues(row idrf.Row) ([]interface{}, error) {
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
	case expected == idrf.IDRFInteger32 || expected == idrf.IDRFInteger64:
		valueAsStr := fmt.Sprintf("%v", rawValue)
		return valueAsStr
	case expected == idrf.IDRFDouble || expected == idrf.IDRFSingle:
		valueAsStr := fmt.Sprintf("%v", rawValue)
		return valueAsStr
	default:
		return rawValue
	}
}
