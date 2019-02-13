package influx

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

type fetchDataSetTestCase struct {
	measures      []string
	measuresErr   error
	dataSet       *idrf.DataSetInfo
	dataSetErr    error
	expectedError bool
}

func TestFetchDataSet(t *testing.T) {
	// Given mock values used in the test cases
	genericError := fmt.Errorf("generic error")

	// START - Expected data set and it's columns
	measures := []string{"a"}
	badMeasures := []string{"b"}
	tag, _ := idrf.NewColumn("tag1", idrf.IDRFString)
	field, _ := idrf.NewColumn("field1", idrf.IDRFBoolean)
	time, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	columns := []*idrf.ColumnInfo{time, tag, field}

	dataSet, _ := idrf.NewDataSet("", "a", columns, "time")
	// END - Expected data set and it's columns

	// Test cases
	cases := []fetchDataSetTestCase{
		{measuresErr: genericError, expectedError: true},
		{measures: badMeasures, expectedError: true},
		{measures: measures, dataSetErr: genericError, expectedError: true},
		{measures: measures, dataSet: dataSet},
	}

	for _, testCase := range cases {
		mockMock := &mockExplorer{
			dataSets:    []*idrf.DataSetInfo{testCase.dataSet},
			dataSetsErr: []error{testCase.dataSetErr},
			measure:     testCase.measures,
			measureErr:  testCase.measuresErr,
		}

		manager := &influxSchemaManager{
			measureExplorer:    mockMock,
			dataSetConstructor: mockMock,
		}
		results, err := manager.FetchDataSet("", "a")
		if testCase.expectedError && err == nil {
			t.Error("expected test case to have an error, no error returned")
		} else if !testCase.expectedError && err != nil {
			t.Errorf("expected test case to have no error.\ngot: %v", err)
		}

		if testCase.expectedError {
			continue
		}

		if results != dataSet {
			t.Errorf("expected: %v\ngot: %v", dataSet, results)
		}

	}

}
