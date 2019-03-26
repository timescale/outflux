package discovery

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"

	influx "github.com/influxdata/influxdb/client/v2"
)

type executeShowQueryFn = func(influxClient influx.Client, database, query string) (*influxqueries.InfluxShowResult, error)

type testCase struct {
	expectedError    bool
	showQueryResult  *influxqueries.InfluxShowResult
	showQueryError   error
	expectedMeasures []string
	expectedTags     []*idrf.Column
}

func TestNewMeasureExplorer(t *testing.T) {
	NewMeasureExplorer(nil)
}
func TestFetchAvailableMeasurements(t *testing.T) {
	var mockClient influx.Client
	mockClient = &influxqueries.MockClient{}
	database := "database"

	cases := []testCase{
		{
			expectedError:  true,
			showQueryError: fmt.Errorf("error executing query"),
		}, { // empty result returned
			expectedError: false,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{},
			},
			expectedMeasures: []string{},
		}, { // result has more than one column
			expectedError: true,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{{"1", "2"}},
			},
			showQueryError: fmt.Errorf("too many columns"),
		}, {
			expectedError: false,
			showQueryResult: &influxqueries.InfluxShowResult{ // result is proper
				Values: [][]string{
					{"1"},
				},
			},
			expectedMeasures: []string{"1"},
		},
	}

	for _, testC := range cases {
		measureExplorer := defaultMeasureExplorer{
			queryService: mock(testC),
		}

		result, err := measureExplorer.FetchAvailableMeasurements(mockClient, database)
		if err != nil && !testC.expectedError {
			t.Errorf("no error expected, got: %v", err)
		} else if err == nil && testC.expectedError {
			t.Errorf("expected error, none received")
		}

		if testC.expectedError {
			continue
		}

		expected := testC.expectedMeasures
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

type mockQueryService struct {
	sqRes *influxqueries.InfluxShowResult
	sqErr error
}

func (m *mockQueryService) ExecuteQuery(client influx.Client, database, command string) ([]influx.Result, error) {
	panic("should not come here")
}

func (m *mockQueryService) ExecuteShowQuery(influxClient influx.Client, database, query string) (*influxqueries.InfluxShowResult, error) {
	return m.sqRes, m.sqErr
}

func mock(tc testCase) influxqueries.InfluxQueryService {
	return &mockQueryService{
		sqRes: tc.showQueryResult, sqErr: tc.showQueryError,
	}
}
