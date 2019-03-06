package combinetojson

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestPrepare(t *testing.T) {
	colsBefore := []*idrf.ColumnInfo{
		&idrf.ColumnInfo{Name: "col1", DataType: idrf.IDRFTimestamp},
		&idrf.ColumnInfo{Name: "col2", DataType: idrf.IDRFBoolean},
		&idrf.ColumnInfo{Name: "col3", DataType: idrf.IDRFInteger32},
	}
	originDs, _ := idrf.NewDataSet("ds", colsBefore, colsBefore[0].Name)
	cols := []*idrf.ColumnInfo{colsBefore[0], &idrf.ColumnInfo{Name: "col2", DataType: idrf.IDRFJson}}
	testCases := []struct {
		desc      string
		ds        *idrf.DataSet
		toCombine map[string]bool
		res       string
		expectErr bool
		val       validator
		comb      columnCombiner
	}{
		{
			desc:      "invalid transformation",
			expectErr: true,
			val:       &mock{valErr: fmt.Errorf("error")},
		}, {
			desc:      "bad origin data set = error from NewDataSet",
			expectErr: true,
			ds:        &idrf.DataSet{},
			val:       &mock{},
			comb:      &mock{combRes: cols},
		}, {
			desc:      "all good",
			res:       "col2",
			toCombine: map[string]bool{},
			ds:        originDs,
			val:       &mock{},
			comb:      &mock{combRes: cols},
		},
	}

	for _, tc := range testCases {
		trans := &Transformer{
			columnsToCombine: tc.toCombine,
			resultColumn:     tc.res,
			validator:        tc.val,
			colColmbiner:     tc.comb,
		}

		in := &idrf.Bundle{
			DataDef: tc.ds,
		}

		bund, err := trans.Prepare(in)

		if tc.expectErr && err == nil {
			t.Errorf("test: %s\nexpected error, none got", tc.desc)
		} else if !tc.expectErr && err != nil {
			t.Errorf("test: %s\nunexpected error: %v", tc.desc, err)
		}

		if tc.expectErr {
			continue
		}

		if trans.cachedInputBundle == nil {
			t.Errorf("test: %s\ninput bundle wasn't cached", tc.desc)
		} else if bund.DataChan == nil {
			t.Errorf("test: %s\noutput data channel not created", tc.desc)
		} else if bund.DataDef.DataSetName != in.DataDef.DataSetName || bund.DataDef.TimeColumn != in.DataDef.TimeColumn {
			t.Errorf("test: %s\noutput data set, did not match expectations", tc.desc)
		}
	}
}

type mock struct {
	valErr  error
	combRes []*idrf.ColumnInfo
}

func (m *mock) validate(originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error {
	return m.valErr
}

func (m *mock) combine(columns []*idrf.ColumnInfo, toCombine map[string]bool, result string) []*idrf.ColumnInfo {
	return m.combRes
}
