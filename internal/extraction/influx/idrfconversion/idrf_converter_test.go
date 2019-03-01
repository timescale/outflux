package idrfconversion

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestConvertByType(t *testing.T) {
	tcs := []struct {
		inVal       interface{}
		inType      idrf.DataType
		expected    interface{}
		isConverted bool
	}{
		{json.Number("1"), idrf.IDRFInteger32, int32(1), true},
		{json.Number("1"), idrf.IDRFInteger64, int64(1), true},
		{json.Number("1.0"), idrf.IDRFSingle, float32(1), true},
		{json.Number("1"), idrf.IDRFDouble, float64(1), true},
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

		expectedType := reflect.TypeOf(tc.expected)
		gotType := reflect.TypeOf(res)
		if expectedType != gotType {
			t.Errorf("expected type: %v\ngot: %v", expectedType, gotType)
		}
	}
}

func TestConvertValues(t *testing.T) {
	testIn := make([]interface{}, 1)
	testIn[0] = "1"
	cols := []*idrf.ColumnInfo{(&idrf.ColumnInfo{DataType: idrf.IDRFString})}
	tcs := []struct {
		in        idrf.Row
		ds        *idrf.DataSet
		expectErr bool
	}{
		{in: make([]interface{}, 1), ds: &idrf.DataSet{}, expectErr: true},
		{in: make([]interface{}, 0), ds: &idrf.DataSet{}, expectErr: false},
		{in: []interface{}{"1"}, ds: &idrf.DataSet{Columns: cols}, expectErr: false},
	}

	for _, tc := range tcs {
		conv := &defaultIdrfConverter{dataSet: tc.ds}
		res, err := conv.Convert(tc.in)
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
