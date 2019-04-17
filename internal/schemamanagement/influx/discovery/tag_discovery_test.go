package discovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

func TestNewTagExplorer(t *testing.T) {
	NewTagExplorer(nil)
}

func TestDiscoverMeasurementTags(t *testing.T) {
	var mockClient influx.Client
	mockClient = &influxqueries.MockClient{}
	database := "database"
	measure := "measure"

	cases := []testCase{
		{
			expectedError:  true,
			showQueryError: fmt.Errorf("error executing query"),
		}, { // empty result returned
			expectedError: false,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{},
			},
		}, { // result has more than one column
			expectedError: true,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "2"},
				},
			},
		}, {
			expectedError: false,
			showQueryResult: &influxqueries.InfluxShowResult{ // result is proper
				Values: [][]string{
					{"1"},
				},
			},
			expectedTags: []*idrf.Column{
				{
					Name:     "1",
					DataType: idrf.IDRFString,
				},
			},
		},
	}

	for _, testCase := range cases {
		tagExplorer := defaultTagExplorer{
			queryService: mock(testCase),
		}

		result, err := tagExplorer.DiscoverMeasurementTags(mockClient, database, measure)
		if err != nil && !testCase.expectedError {
			t.Errorf("did not еxpected error. got '%v'", err)
		} else if err == nil && testCase.expectedError {
			t.Error("еxpected error, none received")
		}

		if testCase.expectedError {
			continue
		}

		expected := testCase.expectedTags
		if len(expected) != len(result) {
			t.Errorf("еxpected result: '%v', got '%v'", expected, result)
		}

		for index, resultColumn := range result {
			if resultColumn.Name != expected[index].Name || resultColumn.DataType != expected[index].DataType {
				t.Errorf("Expected column: %v, got %v", expected[index], resultColumn)
			}
		}
	}
}

func TestFetchMeasurementsShowTagsQuery(t *testing.T) {
	testCases := []struct {
		expectedQuery string
		measure       string
		db            string
	}{
		{
			expectedQuery: `SHOW TAG KEYS FROM "measure"`,
			measure:       "measure",
			db:            "db",
		}, {
			expectedQuery: `SHOW TAG KEYS FROM "measure 1"`,
			measure:       "measure 1",
			db:            "db",
		}, {
			expectedQuery: `SHOW TAG KEYS FROM "measure-2"`,
			measure:       "measure-2",
			db:            "db",
		}, {
			expectedQuery: `SHOW TAG KEYS FROM "rp"."measure-2"`,
			measure:       "rp.measure-2",
			db:            "db",
		},
	}
	for _, tc := range testCases {
		mockClient := &influxqueries.MockClient{}
		queryService := &mockQueryServiceTD{
			expectedDb: tc.db,
			expectedQ:  tc.expectedQuery,
		}

		tagExplorer := defaultTagExplorer{
			queryService: queryService,
		}

		_, err := tagExplorer.fetchMeasurementTags(mockClient, tc.db, tc.measure)
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
	}
}

type mockQueryServiceTD struct {
	expectedQ  string
	expectedDb string
}

func (m *mockQueryServiceTD) ExecuteQuery(client influx.Client, database, command string) ([]influx.Result, error) {
	return nil, nil
}

func (m *mockQueryServiceTD) ExecuteShowQuery(influxClient influx.Client, database, query string) (*influxqueries.InfluxShowResult, error) {
	if m.expectedDb != database || m.expectedQ != query {
		return nil, fmt.Errorf("expected db '%s' and measure '%s', got '%s' and '%s'", m.expectedDb, m.expectedQ, database, query)
	}
	return &influxqueries.InfluxShowResult{
		Values: [][]string{},
	}, nil
}
