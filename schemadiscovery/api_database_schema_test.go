package schemadiscovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// Results or errors returned by the mocked functions
type apiDbTestCase struct {
	influxClientError     error
	influxClient          influx.Client
	fetchedMeasurements   []string
	discoveredTags        []*idrf.ColumnInfo
	discoveredFields      []*idrf.ColumnInfo
	constructDataSetError error
	// flag to indicate whether an error was expected from this test case, or something went wrong
	errorExpected bool
}

func TestInfluxDatabaseSchema(t *testing.T) {
	// Given mock values used in the test cases
	connectionParams := clientutils.ConnectionParams{}
	database := "database"
	genericError := fmt.Errorf("generic error")

	var mockClient influx.Client
	mockClient = &clientutils.MockClient{}

	measures := []string{"a", "b"}
	tag, _ := idrf.NewColumn("tag1", idrf.IDRFString)
	tags := []*idrf.ColumnInfo{
		tag,
	}

	field, _ := idrf.NewColumn("field1", idrf.IDRFBoolean)
	fields := []*idrf.ColumnInfo{
		field,
	}

	time, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	columns := []*idrf.ColumnInfo{time, tag, field}
	dataSetA, _ := idrf.NewDataSet("a", columns)
	dataSetB, _ := idrf.NewDataSet("b", columns)
	dataSets := []*idrf.DataSetInfo{dataSetA, dataSetB}
	// Test cases
	cases := []apiDbTestCase{
		// client is not created
		{influxClientError: genericError, errorExpected: true},
		// couldn't construct data set
		{influxClient: mockClient, constructDataSetError: genericError, errorExpected: true},
		{ // proper response
			influxClient:        mockClient,
			fetchedMeasurements: measures,
			discoveredTags:      tags,
			discoveredFields:    fields,
			errorExpected:       false,
		},
	}

	oldCreateInfluxClient := apiFunctions.createInfluxClient
	oldFetchMeasurements := apiFunctions.fetchMeasurements
	oldDiscoverFields := apiFunctions.discoverFields
	oldDiscoverTags := apiFunctions.discoverTags

	for _, testCase := range cases {
		apiFunctions.createInfluxClient = dbMockCreateClient(&testCase)
		apiFunctions.fetchMeasurements = dbMockFetchMeasurements(&testCase)
		apiFunctions.discoverTags = dbMockColumns(testCase.discoveredTags)
		apiFunctions.discoverFields = dbMockColumns(testCase.discoveredFields)

		results, err := InfluxDatabaseSchema(&connectionParams, database)
		if testCase.errorExpected && err == nil {
			t.Error("expected test case to have an error, no error returned")
		} else if testCase.errorExpected && err != nil {
			continue
		} else if !testCase.errorExpected && err != nil {
			t.Errorf("expected test case to have no error. got error: %v", err)
		} else {
			for i, result := range results {
				dataSet := dataSets[i]

				if result.DataSetName != dataSet.DataSetName {
					t.Errorf("expected data set name: %s, got: %s", dataSet.DataSetName, result.DataSetName)
				}

				for j, col := range result.Columns {
					expectedCol := dataSet.Columns[j]
					if expectedCol.Name != col.Name || expectedCol.DataType != col.DataType {
						t.Errorf("expected column: %v, got: %v", expectedCol, col)
					}
				}
			}
		}
	}

	apiFunctions.createInfluxClient = oldCreateInfluxClient
	apiFunctions.fetchMeasurements = oldFetchMeasurements
	apiFunctions.discoverFields = oldDiscoverFields
	apiFunctions.discoverTags = oldDiscoverTags
}

func dbMockCreateClient(testCase *apiDbTestCase) func(*clientutils.ConnectionParams) (influx.Client, error) {
	return func(*clientutils.ConnectionParams) (influx.Client, error) {
		return testCase.influxClient, testCase.influxClientError
	}
}

func dbMockFetchMeasurements(testCase *apiDbTestCase) func(influx.Client, string) ([]string, error) {
	return func(influx.Client, string) ([]string, error) {
		return testCase.fetchedMeasurements, testCase.constructDataSetError
	}
}

func dbMockColumns(toReturn []*idrf.ColumnInfo) func(influx.Client, string, string) ([]*idrf.ColumnInfo, error) {
	return func(influx.Client, string, string) ([]*idrf.ColumnInfo, error) {
		return toReturn, nil
	}
}
