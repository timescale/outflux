package ingestion

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/idrf"
)

func TestConvertByType(t *testing.T) {
	tcs := []struct {
		inVal       interface{}
		inType      idrf.DataType
		expected    interface{}
		isConverted bool
	}{
		{1, idrf.IDRFInteger32, "1", true},
		{1, idrf.IDRFInteger64, "1", true},
		{1.0, idrf.IDRFSingle, "1", true},
		{1, idrf.IDRFDouble, "1", true},
		{"1", idrf.IDRFString, "1", false},
		{nil, idrf.IDRFBoolean, nil, false},
	}

	for _, tc := range tcs {
		res := convertByType(tc.inVal, tc.inType)
		if tc.inVal == nil {
			if res != nil {
				t.Errorf("nil expected, got: %v", res)
			} else {
				continue
			}
		}
		if tc.isConverted && fmt.Sprintf("%v", tc.expected) != res {
			t.Errorf("expected: %v\ngot: %v", tc.expected, res)
		} else if !tc.isConverted && res.(string) != tc.expected.(string) {
			t.Errorf("expected: %v\ngot: %v", tc.expected, res)
		}
	}
}

func TestConvertValues(t *testing.T) {
	testIn := make([]interface{}, 1)
	testIn[0] = "1"
	cols := []*idrf.ColumnInfo{(&idrf.ColumnInfo{DataType: idrf.IDRFString})}
	tcs := []struct {
		in        idrf.Row
		ds        *idrf.DataSetInfo
		expectErr bool
	}{
		{in: make([]interface{}, 1), ds: &idrf.DataSetInfo{}, expectErr: true},
		{in: make([]interface{}, 0), ds: &idrf.DataSetInfo{}, expectErr: false},
		{in: []interface{}{"1"}, ds: &idrf.DataSetInfo{Columns: cols}, expectErr: false},
	}

	for _, tc := range tcs {
		conv := &defaultIdrfConverter{dataSet: tc.ds}
		res, err := conv.ConvertValues(tc.in)
		if tc.expectErr && err == nil {
			t.Error("expected an error, none received")
		}

		if !tc.expectErr && err != nil {
			t.Errorf("didn't expected error, got: %v\n", err)
		}

		if tc.expectErr {
			continue
		}

		if len(res) != len(tc.in) {
			t.Errorf("result is different length than input.\nexpected: %d, got: %d", len(tc.in), len(res))
		}

		if len(res) == 0 {
			continue
		}

		converted := res[0].(string)
		if converted != tc.in[0] {
			t.Errorf("expected: %v\ngot: %v", tc.in[0], converted)
		}
	}
}
