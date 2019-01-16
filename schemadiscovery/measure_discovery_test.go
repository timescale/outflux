package schemadiscovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

func TestFetchAvailableMeasurements(t *testing.T) {
	var mockClient influx.Client
	mockClient = clientutils.MockClient{}
	database := "database"

	cases := []struct {
		showQueryResult *clientutils.InfluxShowResult
		showQueryError  error
		expectedResult  []string
	}{
		{
			showQueryError: fmt.Errorf("error executing query"),
		}, { // empty result returned
			showQueryResult: &clientutils.InfluxShowResult{
				Values: [][]string{},
			},
			expectedResult: []string{},
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
			expectedResult: []string{"1"},
		},
	}

	oldExecuteQueryFn := mdFunctions.executeShowQuery
	for _, testCase := range cases {
		mdFunctions.executeShowQuery = func(influxClient *influx.Client, database, query string) (*clientutils.InfluxShowResult, error) {
			if testCase.showQueryResult != nil {
				return testCase.showQueryResult, nil
			}

			return nil, testCase.showQueryError
		}

		result, err := FetchAvailableMeasurements(&mockClient, database)
		if err != nil && testCase.showQueryError == nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		} else if err == nil && testCase.showQueryError != nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testCase.showQueryError, err)
		}

		expected := testCase.expectedResult
		if len(expected) != len(result) {
			t.Errorf("еxpected result: '%v', got '%v'", expected, result)
		}

		for index, measureName := range result {
			if measureName != expected[index] {
				t.Errorf("Expected measure: %s, got %s", expected[index], measureName)
			}
		}
	}
	mdFunctions.executeShowQuery = oldExecuteQueryFn
}

/*
func FetchAvailableMeasurements(influxClient *influx.Client, database string) ([]string, error) {
	result, err := mdFunctions.executeShowQuery(influxClient, database, showMeasurementsQuery)

	if err != nil {
		return nil, err
	}

	measureNames := make([]string, len(result.Values))
	for index, valuesRow := range result.Values {
		if len(valuesRow) != 1 {
			errorString := "measurement discovery query returned unexpected result. " +
				"measurement names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		measureNames[index] = valuesRow[0]
	}

	return measureNames, nil
}

*/
