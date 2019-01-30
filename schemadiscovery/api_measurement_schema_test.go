package schemadiscovery

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
)

// Results or errors returned by the mocked functions
type apiTestCase struct {
	influxClientError      error
	influxClient           influx.Client
	fetchMeasurementsError error
	fetchedMeasurements    []string
	discoverTagsError      error
	discoveredTags         []*idrf.ColumnInfo
	discoverFieldsError    error
	discoveredFields       []*idrf.ColumnInfo
	reqMeasure             string
	// flag to indicate whether an error was expected from this test case, or something went wrong
	errorExpected bool
}

func TestInfluxMeasurementSchema(t *testing.T) {
	// Given mock values used in the test cases
	connectionParams := clientutils.ConnectionParams{}
	database := "database"
	genericError := fmt.Errorf("generic error")

	var mockClient influx.Client
	mockClient = &clientutils.MockClient{}

	// START - Expected data set and it's columns
	measures := []string{"a"}
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
	dataSet, _ := idrf.NewDataSet("a", columns, "time")
	// END - Expected data set and it's columns

	// Test cases
	cases := []apiTestCase{
		// client is not created
		{influxClientError: genericError, errorExpected: true},
		// couldn't fetch measurements
		{influxClient: mockClient, fetchMeasurementsError: genericError, errorExpected: true},
		// required measurement not in db
		{influxClient: mockClient, fetchedMeasurements: []string{"wrong"}, reqMeasure: "a", errorExpected: true},
		{ // error fetching tags
			influxClient:        mockClient,
			fetchedMeasurements: measures,
			reqMeasure:          "a",
			discoverTagsError:   genericError,
			errorExpected:       true,
		}, { // error fetching fields
			influxClient:        mockClient,
			fetchedMeasurements: measures,
			reqMeasure:          "a",
			discoveredTags:      tags,
			discoverFieldsError: genericError,
			errorExpected:       true,
		}, { // proper response
			influxClient:        mockClient,
			fetchedMeasurements: measures,
			reqMeasure:          "a",
			discoveredTags:      tags,
			discoveredFields:    fields,
			errorExpected:       false,
		},
	}

	for _, testCase := range cases {
		mockExplorer := mockAllExplorer{
			measures: mockFetchMeasurements(&testCase),
			fields:   mockDiscoverFields(&testCase),
			tags:     mockDiscoverTags(&testCase),
			client:   mockClientME(testCase.influxClient, testCase.influxClientError),
		}

		schemaExplorer := defaultInfluxMeasurementSchemaExplorer{
			clientUtils:     clientutils.NewUtilsWith(&mockExplorer, nil),
			measureExplorer: &mockExplorer,
			tagExplorer:     &mockExplorer,
			fieldExplorer:   &mockExplorer,
		}

		result, err := schemaExplorer.InfluxMeasurementSchema(&connectionParams, database, testCase.reqMeasure)
		if testCase.errorExpected && err == nil {
			t.Error("expected test case to have an error, no error returned")
		} else if testCase.errorExpected && err != nil {
			continue
		} else if !testCase.errorExpected && err != nil {
			t.Errorf("expected test case to have no error. got error: %v", err)
		} else {
			if result.DataSetName != dataSet.DataSetName {
				t.Errorf("expected data set name: %s, got: %s", dataSet.DataSetName, result.DataSetName)
			}

			for i, col := range result.Columns {
				expectedCol := dataSet.Columns[i]
				if expectedCol.Name != col.Name || expectedCol.DataType != col.DataType {
					t.Errorf("expected column: %v, got: %v", expectedCol, col)
				}
			}
		}
	}

}

type mockAllExplorer struct {
	fields   func() ([]*idrf.ColumnInfo, error)
	tags     func() ([]*idrf.ColumnInfo, error)
	measures func() ([]string, error)
	client   func() (influx.Client, error)
}

func (me *mockAllExplorer) FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error) {
	return me.measures()
}

func (me *mockAllExplorer) DiscoverMeasurementTags(influxClient influx.Client, database, measure string) ([]*idrf.ColumnInfo, error) {
	return me.tags()
}

func (me *mockAllExplorer) DiscoverMeasurementFields(
	influxClient influx.Client,
	database string,
	measurement string,
) ([]*idrf.ColumnInfo, error) {
	return me.fields()
}

func (me *mockAllExplorer) CreateInfluxClient(params *clientutils.ConnectionParams) (influx.Client, error) {
	return me.client()
}
func mockCreateClient(testCase *apiTestCase) func(*clientutils.ConnectionParams) (influx.Client, error) {
	return func(*clientutils.ConnectionParams) (influx.Client, error) {
		return testCase.influxClient, testCase.influxClientError
	}
}

func mockFetchMeasurements(testCase *apiTestCase) func() ([]string, error) {
	return func() ([]string, error) {
		return testCase.fetchedMeasurements, testCase.fetchMeasurementsError
	}
}

func mockDiscoverTags(testCase *apiTestCase) func() ([]*idrf.ColumnInfo, error) {
	return func() ([]*idrf.ColumnInfo, error) {
		return testCase.discoveredTags, testCase.discoverTagsError
	}
}

func mockDiscoverFields(testCase *apiTestCase) func() ([]*idrf.ColumnInfo, error) {
	return func() ([]*idrf.ColumnInfo, error) {
		return testCase.discoveredFields, testCase.discoverFieldsError
	}
}

func mockClientME(toReturn influx.Client, errToReturn error) func() (influx.Client, error) {
	return func() (influx.Client, error) {
		return toReturn, errToReturn
	}
}
