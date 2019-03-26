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
