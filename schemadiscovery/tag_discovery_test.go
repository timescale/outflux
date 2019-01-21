package schemadiscovery

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"

	influx "github.com/influxdata/influxdb/client/v2"
)

func TestDiscoverMeasurementTags(t *testing.T) {
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
		}, { // empty result returned
			showQueryResult: &clientutils.InfluxShowResult{
				Values: [][]string{},
			},
		}, { // result has more than one column
			showQueryResult: &clientutils.InfluxShowResult{
				Values: [][]string{
					[]string{"1", "2"},
				},
			},
			showQueryError: fmt.Errorf("too many columns"),
		}, {
			showQueryResult: &clientutils.InfluxShowResult{ // result is proper
				Values: [][]string{
					[]string{"1"},
				},
			},
			expectedResult: []*idrf.ColumnInfo{
				&idrf.ColumnInfo{
					Name:     "1",
					DataType: idrf.IDRFString,
				},
			},
		},
	}

	oldExecuteQueryFn := tdFunctions.executeShowQuery

	for _, testCase := range cases {
		tdFunctions.executeShowQuery = func(influxClient influx.Client, database, query string) (*clientutils.InfluxShowResult, error) {
			if testCase.showQueryResult != nil {
				return testCase.showQueryResult, nil
			}

			return nil, testCase.showQueryError
		}

		result, err := DiscoverMeasurementTags(mockClient, database, measure)
		if err != nil && testCase.showQueryError == nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		} else if err == nil && testCase.showQueryError != nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		}

		expected := testCase.expectedResult
		if len(expected) != len(result) {
			t.Errorf("еxpected result: '%v', got '%v'", expected, result)
		}

		for index, resultColumn := range result {
			if resultColumn.Name != expected[index].Name || resultColumn.DataType != expected[index].DataType {
				t.Errorf("Expected column: %v, got %v", expected[index], resultColumn)
			}
		}
	}
	tdFunctions.executeShowQuery = oldExecuteQueryFn
}
