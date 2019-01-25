package config

import "testing"

type testCase struct {
	db      string
	measure string
	from    string
	to      string
	chunk   uint
	limit   uint
}

func TestNewMeasureExtractionConfig(t *testing.T) {
	badCases := []testCase{
		{db: "", measure: "measure", chunk: 1},
		{db: "db", measure: "", chunk: 1},
		{db: "db", measure: "measure", chunk: 0},
		{db: "db", measure: "measure", from: "2019-01-01T00:00:00", chunk: 1},
		{db: "db", measure: "measure", from: "2019-01-01", chunk: 1},
		{db: "db", measure: "measure", to: "2019-01-01T00:00:00", chunk: 1},
		{db: "db", measure: "measure", to: "2019-01-01", chunk: 1},
	}

	for _, badCase := range badCases {
		_, err := NewMeasureExtractionConfig(
			badCase.db,
			badCase.measure,
			badCase.chunk,
			badCase.limit,
			badCase.from,
			badCase.to,
		)

		if err == nil {
			t.Error("expected an error, no error received")
		}
	}

	goodCases := []testCase{
		{db: "db", measure: "measure", chunk: 1},
		{db: "db", measure: "measure", chunk: 1, limit: 1},
		{db: "db", measure: "measure", chunk: 1, from: "2019-01-01T00:00:00Z"},
		{db: "db", measure: "measure", chunk: 1, from: "2019-01-01T00:00:00+00:00"},
		{db: "db", measure: "measure", chunk: 1, from: "2019-01-01T00:00:00-01:00"},
		{db: "db", measure: "measure", chunk: 1, to: "2019-01-01T00:00:00-01:00"},
		{db: "db", measure: "measure", chunk: 1, from: "2019-01-01T00:00:00-01:00", to: "2019-01-01T00:00:00+01:00"},
	}

	for _, goodCase := range goodCases {
		config, err := NewMeasureExtractionConfig(
			goodCase.db,
			goodCase.measure,
			goodCase.chunk,
			goodCase.limit,
			goodCase.from,
			goodCase.to,
		)

		if err != nil {
			t.Errorf("expected not error, got: %v", err)
		}

		if config.ChunkSize != goodCase.chunk || config.Database != goodCase.db ||
			config.Measure != goodCase.measure || config.From != goodCase.from ||
			config.To != goodCase.to || config.Limit != goodCase.limit {
			t.Errorf("config not good. expected values: %v, got: %v", goodCase, config)
		}
	}
}
