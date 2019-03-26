package jsoncombiner

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestCombiner(t *testing.T) {
	cols := []*idrf.Column{
		{Name: "col1", DataType: idrf.IDRFBoolean},
		{Name: "col2", DataType: idrf.IDRFDouble},
		{Name: "col3", DataType: idrf.IDRFInteger32},
		{Name: "col4", DataType: idrf.IDRFSingle},
	}

	resCol := &idrf.Column{Name: "res", DataType: idrf.IDRFJson}
	testCases := []struct {
		desc      string
		cols      []*idrf.Column
		toCombine map[string]bool
		expect    []*idrf.Column
	}{
		{
			desc:      "combine cols in the middle",
			cols:      cols,
			toCombine: map[string]bool{cols[1].Name: true, cols[2].Name: true},
			expect:    []*idrf.Column{cols[0], resCol, cols[3]},
		}, {
			desc:      "combine cols at end",
			cols:      cols,
			toCombine: map[string]bool{cols[2].Name: true, cols[3].Name: true},
			expect:    []*idrf.Column{cols[0], cols[1], resCol},
		}, {
			desc:      "combine cols at beginning",
			cols:      cols,
			toCombine: map[string]bool{cols[0].Name: true, cols[1].Name: true},
			expect:    []*idrf.Column{resCol, cols[2], cols[3]},
		}, {
			desc:      "combine cols that are not adjacent",
			cols:      cols,
			toCombine: map[string]bool{cols[1].Name: true, cols[3].Name: true},
			expect:    []*idrf.Column{cols[0], resCol, cols[2]},
		},
	}

	combiner := &defColCombiner{}
	for _, tc := range testCases {
		cols := combiner.combine(tc.cols, tc.toCombine, resCol.Name)
		if len(cols) != len(tc.expect) {
			t.Errorf("test: %s\nexpected length: %d, got: %d", tc.desc, len(cols), len(tc.expect))
			continue
		}

		for i, col := range cols {
			if col.Name != tc.expect[i].Name || col.DataType != tc.expect[i].DataType {
				t.Errorf("test: %s\nat position %d expected name:%s, type:%d\ngot name:%s, type:%d",
					tc.desc, i, tc.expect[i].Name, tc.expect[i].DataType, col.Name, col.DataType)
			}
		}
	}
}
