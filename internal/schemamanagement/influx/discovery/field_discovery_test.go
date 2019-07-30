package discovery

import (
	"fmt"
	"reflect"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

func TestDiscoverMeasurementFields(t *testing.T) {
	var mockClient influx.Client
	mockClient = &influxqueries.MockClient{}
	database := "database"
	measure := "measure"
	rp := "autogen"
	cases := []testCase{
		{
			desc:           "not good, error executing query",
			expectedError:  true,
			showQueryError: fmt.Errorf("error executing query"),
		}, {
			desc:          "empty result returned, error should be result, must have fields",
			expectedError: true,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{},
			},
		}, {
			desc:          "result has more than two columns",
			expectedError: true,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "2", "3"},
				},
			},
		}, {
			desc: "proper result",
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "boolean"},
					{"2", "float"},
					{"3", "integer"},
					{"4", "string"},
				},
			},
			expectedTags: []*idrf.Column{
				{Name: "1", DataType: idrf.IDRFBoolean},
				{Name: "2", DataType: idrf.IDRFDouble},
				{Name: "3", DataType: idrf.IDRFInteger64},
				{Name: "4", DataType: idrf.IDRFString},
			},
		}, {
			desc: "same field, diff types, uncastable",
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "boolean"},
					{"1", "float"},
				},
			},
			expectedError: true,
		}, {
			desc:          "same field, diff types, int and float (flag says error)",
			expectedError: true,
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "integer"},
					{"1", "float"},
				},
			},
		}, {
			desc: "same field, diff types, int and float (flag says no error)",
			showQueryResult: &influxqueries.InfluxShowResult{
				Values: [][]string{
					{"1", "integer"},
					{"1", "float"},
				},
			},
			onConflictConvertIntToFloat: true,
			expectedTags: []*idrf.Column{
				{Name: "1", DataType: idrf.IDRFDouble},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.desc, func(t *testing.T) {
			fieldExplorer := defaultFieldExplorer{
				queryService: mock(testCase),
			}
			result, err := fieldExplorer.DiscoverMeasurementFields(mockClient, database, rp, measure, testCase.onConflictConvertIntToFloat)
			if err != nil && !testCase.expectedError {
				t.Errorf("unexpected error %v", err)
			} else if err == nil && testCase.expectedError {
				t.Errorf("expected error, none received")
			}

			if testCase.expectedError {
				return
			}

			expected := testCase.expectedTags
			if len(expected) != len(result) {
				t.Errorf("expected result: '%v', got '%v'", expected, result)
			}

			for index, resColumn := range result {
				if resColumn.Name != expected[index].Name || resColumn.DataType != expected[index].DataType {
					t.Errorf("expected column: %v, got %v", expected[index], resColumn)
				}
			}
		})
	}
}

func TestChooseDataTypeForFields(t *testing.T) {
	testCases := []struct {
		desc                        string
		in                          [][2]string
		out                         map[string]idrf.DataType
		onConflictConvertIntToFloat bool
		expectErr                   bool
	}{
		{
			desc: "All good, single string field",
			in:   [][2]string{{"a", "string"}},
			out:  map[string]idrf.DataType{"a": idrf.IDRFString},
		}, {
			desc: "All good, multiple distinct fields",
			in:   [][2]string{{"a", "string"}, {"b", "integer"}, {"c", "float"}},
			out:  map[string]idrf.DataType{"a": idrf.IDRFString, "b": idrf.IDRFInteger64, "c": idrf.IDRFDouble},
		}, {
			desc:      "Not good, incomparable fields",
			in:        [][2]string{{"a", "string"}, {"b", "integer"}, {"b", "float"}},
			expectErr: true,
		}, {
			desc:                        "Good, incomparable fields, but forced int to float conversion",
			in:                          [][2]string{{"a", "string"}, {"b", "integer"}, {"b", "float"}},
			onConflictConvertIntToFloat: true,
			out:                         map[string]idrf.DataType{"a": idrf.IDRFString, "b": idrf.IDRFDouble},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := chooseDataTypeForFields(tc.in, tc.onConflictConvertIntToFloat)
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if err == nil && tc.expectErr {
				t.Error("unexpected lack of error")
				return
			}
			if !reflect.DeepEqual(got, tc.out) {
				t.Errorf("expected: %v\ngot: %v", tc.out, got)
			}
		})
	}
}
