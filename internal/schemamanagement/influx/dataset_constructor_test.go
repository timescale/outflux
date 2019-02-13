package influx

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"

	influx "github.com/influxdata/influxdb/client/v2"
)

func TestNewDataSetConstructor(t *testing.T) {
	var qs influxqueries.InfluxQueryService
	newDataSetConstructor("", &influxqueries.MockClient{}, qs)
}

func TestConstruct(t *testing.T) {
	genError := fmt.Errorf("generic error")
	tags := []*idrf.ColumnInfo{&idrf.ColumnInfo{Name: "tag", DataType: idrf.IDRFString, ForeignKey: nil}}
	fields := []*idrf.ColumnInfo{&idrf.ColumnInfo{Name: "field", DataType: idrf.IDRFBoolean, ForeignKey: nil}}
	testCases := []struct {
		desc        string
		tags        []*idrf.ColumnInfo
		tagsErr     error
		fields      []*idrf.ColumnInfo
		fieldsErr   error
		expectedErr bool
	}{
		{
			desc:        "Error on discover tags",
			tagsErr:     genError,
			expectedErr: true,
		}, {
			desc:        "Error on discover fields",
			tags:        tags,
			fieldsErr:   genError,
			expectedErr: true,
		}, {
			desc:   "All good",
			tags:   tags,
			fields: fields,
		},
	}

	for _, tc := range testCases {
		mock := &mocker{tags: tc.tags, tagsErr: tc.tagsErr, fields: tc.fields, fieldsErr: tc.fieldsErr}
		constructor := defaultDSConstructor{
			tagExplorer:   mock,
			fieldExplorer: mock,
		}

		res, err := constructor.construct("a")
		if err != nil && !tc.expectedErr {
			t.Errorf("unexpected error %v", err)
		} else if err == nil && tc.expectedErr {
			t.Errorf("expected error, none received")
		}

		if tc.expectedErr {
			continue
		}

		if res.DataSetName != "a" {
			t.Errorf("expected data set to be named: a, got: %s", res.DataSetName)
		}

		if len(res.Columns) != 1+len(tags)+len(fields) { //time, tags, fields
			t.Errorf("exected %d columns, got %d", 1+len(tags)+len(fields), len(res.Columns))
		}

		if res.TimeColumn != res.Columns[0].Name {
			t.Errorf("expectd time column to be first in columns array")
		}
	}
}

type mocker struct {
	tags      []*idrf.ColumnInfo
	tagsErr   error
	fields    []*idrf.ColumnInfo
	fieldsErr error
}

func (m *mocker) DiscoverMeasurementTags(influxClient influx.Client, database, measure string) ([]*idrf.ColumnInfo, error) {
	return m.tags, m.tagsErr
}

func (m *mocker) DiscoverMeasurementFields(influxClient influx.Client, database, measurement string) ([]*idrf.ColumnInfo, error) {
	return m.fields, m.fieldsErr
}
