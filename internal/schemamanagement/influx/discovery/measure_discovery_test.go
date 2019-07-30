package discovery

import (
	"errors"
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"

	influx "github.com/influxdata/influxdb/client/v2"
)

type executeShowQueryFn = func(influxClient influx.Client, database, query string) (*influxqueries.InfluxShowResult, error)

type testCase struct {
	desc                        string
	expectedError               bool
	showQueryResult             *influxqueries.InfluxShowResult
	showQueryError              error
	expectedMeasures            []string
	expectedTags                []*idrf.Column
	fieldsErr                   error
	onConflictConvertIntToFloat bool
}

func TestNewMeasureExplorer(t *testing.T) {
	NewMeasureExplorer(nil, nil)
}

func TestFetchAvailableMeasurements(t *testing.T) {
	var mockClient influx.Client
	mockClient = &influxqueries.MockClient{}
	database := "database"
	rp := "autogen"
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
		}, {
			expectedError: false, // no fields discovered for measure in given rp, measure is not returned
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1"},
				},
			},
			fieldsErr:        errors.New("generic error"),
			expectedMeasures: []string{},
		},
	}

	for _, testC := range cases {
		mock := mock(testC)
		measureExplorer := defaultMeasureExplorer{
			queryService:  mock,
			fieldExplorer: mock,
		}

		result, err := measureExplorer.FetchAvailableMeasurements(mockClient, database, rp, false)
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

type mockAll struct {
	sqRes     *influxqueries.InfluxShowResult
	sqErr     error
	fieldsErr error
}

func (m *mockAll) ExecuteQuery(client influx.Client, database, command string) ([]influx.Result, error) {
	panic("should not come here")
}

func (m *mockAll) ExecuteShowQuery(influxClient influx.Client, database, query string) (*influxqueries.InfluxShowResult, error) {
	return m.sqRes, m.sqErr
}

func (m *mockAll) DiscoverMeasurementFields(c influx.Client, db, rp, ms string, onConflictConvertIntToFloat bool) ([]*idrf.Column, error) {
	return nil, m.fieldsErr
}

func mock(tc testCase) *mockAll {
	return &mockAll{
		sqRes: tc.showQueryResult, sqErr: tc.showQueryError, fieldsErr: tc.fieldsErr,
	}
}
