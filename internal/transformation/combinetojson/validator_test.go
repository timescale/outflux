package combinetojson

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestValidator(t *testing.T) {
	twoCol := []*idrf.ColumnInfo{
		{Name: "col1", DataType: idrf.IDRFTimestamp},
		{Name: "col2", DataType: idrf.IDRFBoolean},
	}

	threeCol := []*idrf.ColumnInfo{
		{Name: "col1", DataType: idrf.IDRFTimestamp},
		{Name: "col2", DataType: idrf.IDRFBoolean},
		{Name: "col3", DataType: idrf.IDRFDouble},
	}
	testCases := []struct {
		desc       string
		originData *idrf.DataSet
		toCombine  map[string]bool
		res        string
		expectErr  bool
	}{
		{
			desc:       "res column named the same as a column not designated for combination",
			expectErr:  true,
			res:        threeCol[1].Name,
			toCombine:  map[string]bool{threeCol[2].Name: true},
			originData: &idrf.DataSet{DataSetName: "ds", Columns: threeCol, TimeColumn: threeCol[0].Name},
		}, {
			desc:       "time column can't be combined",
			originData: &idrf.DataSet{DataSetName: "ds", Columns: twoCol, TimeColumn: twoCol[0].Name},
			toCombine:  map[string]bool{twoCol[0].Name: true},
			res:        "res",
			expectErr:  true,
		}, {
			desc:       "column to be combined not in data set",
			originData: &idrf.DataSet{DataSetName: "ds", Columns: twoCol, TimeColumn: twoCol[0].Name},
			toCombine:  map[string]bool{twoCol[1].Name + "wrong": true},
			res:        "res",
			expectErr:  true,
		}, {
			desc:       "all ok",
			originData: &idrf.DataSet{DataSetName: "ds", Columns: threeCol, TimeColumn: threeCol[0].Name},
			toCombine:  map[string]bool{threeCol[1].Name: true, threeCol[2].Name: true},
			res:        "res",
		},
	}

	val := &defValidator{}
	for _, testCase := range testCases {
		err := val.validate(testCase.originData, testCase.res, testCase.toCombine)
		if err == nil && testCase.expectErr {
			t.Errorf("test:%s\nexpected error, none got", testCase.desc)
		} else if err != nil && !testCase.expectErr {
			t.Errorf("test:%s\nunexpected error: %v", testCase.desc, err)
		}
	}
}
