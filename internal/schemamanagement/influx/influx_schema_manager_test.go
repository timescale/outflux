package influx

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"

	influx "github.com/influxdata/influxdb/client/v2"
)

func TestNewInfluxSchemaManager(t *testing.T) {
	client := &influxqueries.MockClient{}
	db := "db"
	NewSchemaManager(client, db, nil)
}

func TestDiscoverDataSets(t *testing.T) {
	mock := &ismMeasureExp{measureErr: fmt.Errorf("error")}
	sm := &SchemaManager{
		measureExplorer: mock,
	}
	res, err := sm.DiscoverDataSets()
	if err == nil {
		t.Errorf("expected error, none received")
	}

	mock.measureErr = nil
	mock.measures = []string{"a"}
	res, err = sm.DiscoverDataSets()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if res[0] != "a" {
		t.Errorf("expected: 'a' got '%v'", res)
	}
}

func TestFetchDataSet(t *testing.T) {
	// Given mock values used in the test cases
	genericError := fmt.Errorf("generic error")
	badMeasure := "b"
	goodMeasure := "a"
	measures := []string{goodMeasure}
	dataSet := &idrf.DataSetInfo{DataSetName: goodMeasure}
	// Test cases
	cases := []struct {
		desc       string
		expectErr  bool
		measures   []string
		reqMeasure string
		msErr      error
		dsErr      error
		ds         *idrf.DataSetInfo
	}{
		{desc: "error discovering measures", expectErr: true, msErr: genericError},
		{desc: "requested measure is missing", expectErr: true, measures: measures, reqMeasure: badMeasure},
		{desc: "error constructing data set", expectErr: true, measures: measures, reqMeasure: goodMeasure, dsErr: genericError},
		{desc: "good data set", measures: measures, reqMeasure: goodMeasure, ds: dataSet},
	}

	for _, testCase := range cases {
		mockMExp := &ismMeasureExp{measures: testCase.measures, measureErr: testCase.msErr}
		mockDSCons := &ismDSCons{dsErr: testCase.dsErr, ds: testCase.ds}
		manager := &SchemaManager{measureExplorer: mockMExp, dataSetConstructor: mockDSCons}
		res, err := manager.FetchDataSet(testCase.reqMeasure)
		if testCase.expectErr && err == nil {
			t.Error("expected test case to have an error, no error returned")
		} else if !testCase.expectErr && err != nil {
			t.Errorf("unexpected err: %v", err)
		}

		if testCase.expectErr {
			continue
		}
		if res.DataSetName != testCase.ds.DataSetName {
			t.Errorf("expected ds name: %s, got %s", testCase.ds.DataSetName, res.DataSetName)
		}
	}

}

type ismMeasureExp struct {
	measures   []string
	measureErr error
}

func (i *ismMeasureExp) FetchAvailableMeasurements(influxClient influx.Client, database string) ([]string, error) {
	return i.measures, i.measureErr
}

type ismDSCons struct {
	ds    *idrf.DataSetInfo
	dsErr error
}

func (i *ismDSCons) construct(measure string) (*idrf.DataSetInfo, error) {
	return i.ds, i.dsErr
}
