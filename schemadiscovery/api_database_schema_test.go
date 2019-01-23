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

	for _, testCase := range cases {
		mockMock := &mockExplorer{
			measures: dbMockFetchMeasurements(testCase),
			fields:   dbMockColumns(testCase.discoveredFields),
			tags:     dbMockColumns(testCase.discoveredTags),
			client:   dbMockClient(testCase.influxClient, testCase.influxClientError),
		}
		explorer := defaultInfluxDatabaseSchemaExplorer{
			clientUtils:     clientutils.NewUtilsWith(mockMock, nil),
			measureExplorer: mockMock,
			tagExplorer:     mockMock,
			fieldExplorer:   mockMock,
		}

		results, err := explorer.InfluxDatabaseSchema(&connectionParams, database)
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

}

func dbMockCreateClient(testCase *apiDbTestCase) func(*clientutils.ConnectionParams) (influx.Client, error) {
	return func(*clientutils.ConnectionParams) (influx.Client, error) {
		return testCase.influxClient, testCase.influxClientError
	}
}

type mockExplorer struct {
	measures func() ([]string, error)
	fields   func() ([]*idrf.ColumnInfo, error)
	tags     func() ([]*idrf.ColumnInfo, error)
	client   func() (influx.Client, error)
}

func (me *mockExplorer) FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error) {
	return me.measures()
}

func (me *mockExplorer) DiscoverMeasurementTags(influxClient influx.Client, database, measure string) ([]*idrf.ColumnInfo, error) {
	return me.tags()
}

func (me *mockExplorer) DiscoverMeasurementFields(
	influxClient influx.Client,
	database string,
	measurement string,
) ([]*idrf.ColumnInfo, error) {
	return me.fields()
}

func (me *mockExplorer) CreateInfluxClient(params *clientutils.ConnectionParams) (influx.Client, error) {
	return me.client()
}

func dbMockFetchMeasurements(testCase apiDbTestCase) func() ([]string, error) {
	return func() ([]string, error) {
		return testCase.fetchedMeasurements, testCase.constructDataSetError
	}
}

func dbMockColumns(toReturn []*idrf.ColumnInfo) func() ([]*idrf.ColumnInfo, error) {
	return func() ([]*idrf.ColumnInfo, error) {
		return toReturn, nil
	}
}

func dbMockClient(toReturn influx.Client, errToReturn error) func() (influx.Client, error) {
	return func() (influx.Client, error) {
		return toReturn, errToReturn
	}
}
