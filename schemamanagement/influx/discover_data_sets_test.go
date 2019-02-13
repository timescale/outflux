package influx

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
)

// Results or errors returned by the mocked functions
type apiDbTestCase struct {
	measurements []string
	measureErr   error
	dataSets     []*idrf.DataSetInfo
	dataSetsErr  []error
	// flag to indicate whether an error was expected from this test case, or something went wrong
	errorExpected bool
}

func TestDiscoverDataSets(t *testing.T) {
	// Given mock values used in the test cases
	genericError := fmt.Errorf("generic error")

	measures := []string{"a", "b"}
	tag, _ := idrf.NewColumn("tag1", idrf.IDRFString)
	field, _ := idrf.NewColumn("field1", idrf.IDRFBoolean)
	time, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	columns := []*idrf.ColumnInfo{time, tag, field}

	dataSetA, _ := idrf.NewDataSet("", "a", columns, "time")
	dataSetB, _ := idrf.NewDataSet("", "b", columns, "time")
	dataSets := []*idrf.DataSetInfo{dataSetA, dataSetB}
	// Test cases
	cases := []apiDbTestCase{
		// error discovering measures
		{measureErr: genericError, errorExpected: true},
		// error on first data set construction
		{measurements: measures, dataSets: make([]*idrf.DataSetInfo, 1), dataSetsErr: []error{genericError}, errorExpected: true},
		// error on second data set construction
		{measurements: measures, dataSets: []*idrf.DataSetInfo{dataSetA, nil}, dataSetsErr: []error{nil, genericError}, errorExpected: true},
		// all good
		{measurements: measures, dataSets: []*idrf.DataSetInfo{dataSetA, dataSetB}, dataSetsErr: []error{nil, nil}},
	}

	for _, testCase := range cases {
		mockMock := &mockExplorer{
			dataSets: testCase.dataSets, dataSetsErr: testCase.dataSetsErr,
			measure: testCase.measurements, measureErr: testCase.measureErr,
		}
		manager := &influxSchemaManager{
			measureExplorer:    mockMock,
			dataSetConstructor: mockMock,
		}
		results, err := manager.DiscoverDataSets()
		if testCase.errorExpected && err == nil {
			t.Error("expected test case to have an error, no error returned")
		} else if !testCase.errorExpected && err != nil {
			t.Errorf("expected test case to have no error.\ngot: %v", err)
		}

		if testCase.errorExpected {
			continue
		}

		for i, result := range results {
			dataSet := dataSets[i]

			if result != dataSet {
				t.Errorf("expected: %v\ngot: %v", dataSet, result)
			}
		}
	}

}

type mockExplorer struct {
	dataSets       []*idrf.DataSetInfo
	currentDataSet int
	dataSetsErr    []error
	measure        []string
	measureErr     error
}

func (m *mockExplorer) FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error) {
	return m.measure, m.measureErr
}

func (m *mockExplorer) construct(measure string) (*idrf.DataSetInfo, error) {
	toReturn := m.currentDataSet
	m.currentDataSet++
	return m.dataSets[toReturn], m.dataSetsErr[toReturn]
}
