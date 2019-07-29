package ts

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestDataSetToSQLTableDef(t *testing.T) {
	singleCol := []*idrf.Column{{Name: "col1", DataType: idrf.IDRFTimestamp}}
	twoCols := []*idrf.Column{singleCol[0], {Name: "col2", DataType: idrf.IDRFDouble}}
	threeCols := []*idrf.Column{
		{Name: "col1", DataType: idrf.IDRFTimestamptz},
		{Name: "col2", DataType: idrf.IDRFString},
		{Name: "col 3", DataType: idrf.IDRFInteger64}}
	ds1, _ := idrf.NewDataSet("ds1", singleCol, singleCol[0].Name)
	ds2, _ := idrf.NewDataSet("ds2", twoCols, twoCols[0].Name)
	ds3, _ := idrf.NewDataSet("ds 3", threeCols, threeCols[0].Name)
	ds4, _ := idrf.NewDataSet("fake_schema.ds4", singleCol, singleCol[0].Name)
	tcs := []struct {
		ds       *idrf.DataSet
		schema   string
		expected string
	}{
		{ds: ds1, expected: "CREATE TABLE \"ds1\"(\"col1\" TIMESTAMP)"},
		{ds: ds2, expected: "CREATE TABLE \"ds2\"(\"col1\" TIMESTAMP, \"col2\" FLOAT)"},
		{ds: ds3, expected: "CREATE TABLE \"ds 3\"(\"col1\" TIMESTAMPTZ, \"col2\" TEXT, \"col 3\" BIGINT)"},
		{ds: ds4, schema: "schema", expected: "CREATE TABLE \"schema\".\"fake_schema.ds4\"(\"col1\" TIMESTAMP)"},
	}
	for _, tc := range tcs {
		query := dataSetToSQLTableDef(tc.schema, tc.ds)
		if query != tc.expected {
			t.Errorf("expected: %s\ngot: %s", tc.expected, query)
		}
	}
}
