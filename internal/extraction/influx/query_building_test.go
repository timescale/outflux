package influx

import (
	"testing"

	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

func TestBuildMeasurementName(t *testing.T) {
	testCases := []struct {
		in   string
		inRp string
		exp  string
	}{
		{in: "measure", inRp: "", exp: `"measure"`},
		{in: "m.easure", inRp: "rp", exp: `"rp"."m.easure"`},
		{in: "m.measure name", inRp: "r p", exp: `"r p"."m.measure name"`},
		{in: "measure name.measure.name", exp: `"measure name.measure.name"`},
	}

	for _, tc := range testCases {
		out := buildMeasurementName(tc.inRp, tc.in)
		if out != tc.exp {
			t.Errorf("expected: %s, got: %s", tc.exp, out)
		}
	}
}

func TestBuildProjection(t *testing.T) {
	testCases := []struct {
		in  []*idrf.Column
		exp string
	}{
		{in: []*idrf.Column{{Name: "col1"}}, exp: `"col1"`},
		{in: []*idrf.Column{{Name: "col 1"}}, exp: `"col 1"`},
		{in: []*idrf.Column{{Name: "col 1"}, {Name: "col 2"}}, exp: `"col 1", "col 2"`},
	}

	for _, tc := range testCases {
		out := buildProjection(tc.in)
		if out != tc.exp {
			t.Errorf("expected: %s, got: %s", tc.exp, out)
		}
	}
}

func TestBuildSelectCommand(t *testing.T) {
	testCases := []struct {
		measure string
		rp      string
		columns []*idrf.Column
		from    string
		to      string
		limit   uint64
		exp     string
	}{
		{
			measure: "m",
			columns: []*idrf.Column{{Name: "col1"}},
			exp:     `SELECT "col1" FROM "m"`,
		}, {
			measure: "m.m",
			columns: []*idrf.Column{{Name: "col1"}, {Name: "col 2"}},
			from:    "a",
			exp:     `SELECT "col1", "col 2" FROM "m.m" WHERE time >= 'a'`,
		}, {
			measure: "m",
			columns: []*idrf.Column{{Name: "col1"}},
			to:      "b",
			exp:     `SELECT "col1" FROM "m" WHERE time <= 'b'`,
		}, {
			measure: "m",
			columns: []*idrf.Column{{Name: "col1"}},
			from:    "a",
			to:      "b",
			exp:     `SELECT "col1" FROM "m" WHERE time >= 'a' AND time <= 'b'`,
		}, {
			measure: "m",
			columns: []*idrf.Column{{Name: "col1"}},
			limit:   11,
			exp:     `SELECT "col1" FROM "m" LIMIT 11`,
		}, {
			measure: "m",
			rp:      "rep pol",
			columns: []*idrf.Column{{Name: "col1"}},
			from:    "a",
			to:      "b",
			exp:     `SELECT "col1" FROM "rep pol"."m" WHERE time >= 'a' AND time <= 'b'`,
		},
	}

	for _, tc := range testCases {
		config := &config.MeasureExtraction{
			Measure:         tc.measure,
			RetentionPolicy: tc.rp,
			From:            tc.from,
			To:              tc.to,
			Limit:           tc.limit,
		}

		out := buildSelectCommand(config, tc.columns)
		if out != tc.exp {
			t.Errorf("expected: %s, got: %s", tc.exp, out)
		}
	}
}
