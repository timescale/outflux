package schemadiscovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

func TestDiscoverMeasurementFields(t *testing.T) {
	var mockClient influx.Client
	mockClient = &clientutils.MockClient{}
	database := "database"
	measure := "measure"

	cases := []struct {
		showQueryResult *clientutils.InfluxShowResult
		showQueryError  error
		expectedResult  []*idrf.ColumnInfo
	}{
		{
			showQueryError: fmt.Errorf("error executing query"),
		}, { // empty result returned, error should be result, must have fields
			showQueryResult: &clientutils.InfluxShowResult{
				Values: [][]string{},
			},
			showQueryError: fmt.Errorf("wrong result returned"),
		}, { // result has more than two columns
			showQueryResult: &clientutils.InfluxShowResult{
				Values: [][]string{
					[]string{"1", "2", "3"},
				},
			},
			showQueryError: fmt.Errorf("too many columns"),
		}, {
			showQueryResult: &clientutils.InfluxShowResult{ // proper result
				Values: [][]string{
					[]string{"1", "boolean"},
					[]string{"2", "float"},
					[]string{"3", "integer"},
					[]string{"4", "string"},
				},
			},
			expectedResult: []*idrf.ColumnInfo{
				&idrf.ColumnInfo{Name: "1", DataType: idrf.IDRFBoolean},
				&idrf.ColumnInfo{Name: "2", DataType: idrf.IDRFFloating},
				&idrf.ColumnInfo{Name: "3", DataType: idrf.IDRFInteger},
				&idrf.ColumnInfo{Name: "4", DataType: idrf.IDRFString},
			},
		},
	}

	oldExecuteQueryFn := fdFunctions.executeShowQuery

	for _, testCase := range cases {
		fdFunctions.executeShowQuery = func(influxClient influx.Client, database, query string) (*clientutils.InfluxShowResult, error) {
			if testCase.showQueryResult != nil {
				return testCase.showQueryResult, nil
			}

			return nil, testCase.showQueryError
		}

		result, err := DiscoverMeasurementFields(mockClient, database, measure)
		if err != nil && testCase.showQueryError == nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		} else if err == nil && testCase.showQueryError != nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		}

		expected := testCase.expectedResult
		if len(expected) != len(result) {
			t.Errorf("еxpected result: '%v', got '%v'", expected, result)
		}

		for index, resColumn := range result {
			if resColumn.Name != expected[index].Name || resColumn.DataType != expected[index].DataType {
				t.Errorf("Expected column: %v, got %v", expected[index], resColumn)
			}
		}
	}
	fdFunctions.executeShowQuery = oldExecuteQueryFn
}
