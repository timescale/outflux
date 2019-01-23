package discovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

type executeShowQueryFn = func(influxClient influx.Client, database, query string) (*clientutils.InfluxShowResult, error)

type mdTestCase struct {
	showQueryResult *clientutils.InfluxShowResult
	showQueryError  error
	expectedResult  []string
}

func TestFetchAvailableMeasurements(t *testing.T) {
	var mockClient influx.Client
	mockClient = &clientutils.MockClient{}
	database := "database"

	cases := []mdTestCase{
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

	for _, testC := range cases {
		measureExplorer := defaultMeasureExplorer{
			utils: clientutils.NewUtilsWith(nil, mockShowexecutorMD(testC)),
		}

		result, err := measureExplorer.FetchAvailableMeasurements(mockClient, database)
		if err != nil && testC.showQueryError == nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testC.showQueryError, err)
		} else if err == nil && testC.showQueryError != nil {
			t.Errorf("еxpected error to be '%v' got '%v' instead", testC.showQueryError, err)
		}

		expected := testC.expectedResult
		if len(expected) != len(result) {
			t.Errorf("еxpected result: '%v', got '%v'", expected, result)
		}

		for index, measureName := range result {
			if measureName != expected[index] {
				t.Errorf("Expected measure: %s, got %s", expected[index], measureName)
			}
		}
	}
}

type mockShowExecutorMD struct {
	resToReturn *clientutils.InfluxShowResult
	errToReturn error
}

func (cg *mockShowExecutorMD) ExecuteShowQuery(
	influxClient influx.Client,
	database, query string,
) (*clientutils.InfluxShowResult, error) {
	return cg.resToReturn, cg.errToReturn
}

func mockShowexecutorMD(testC mdTestCase) *mockShowExecutorMD {
	if testC.showQueryResult != nil {
		return &mockShowExecutorMD{testC.showQueryResult, nil}
	}

	return &mockShowExecutorMD{nil, testC.showQueryError}
}
