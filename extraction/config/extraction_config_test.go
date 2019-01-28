package config

import (
	"math"
	"testing"
)

func TestNewMeasureExtractionConfig(t *testing.T) {
	badCases := []MeasureExtraction{
		{Database: "", Measure: "measure", ChunkSize: 1},
		{Database: "Db", Measure: "", ChunkSize: 1},
		{Database: "Db", Measure: "measure", ChunkSize: 0},
		{Database: "Db", Measure: "measure", ChunkSize: uint(math.MaxInt64) + uint(1)},
		{Database: "Db", Measure: "measure", From: "2019-01-01T00:00:00", ChunkSize: 1},
		{Database: "Db", Measure: "measure", From: "2019-01-01", ChunkSize: 1},
		{Database: "Db", Measure: "measure", To: "2019-01-01T00:00:00", ChunkSize: 1},
		{Database: "Db", Measure: "measure", To: "2019-01-01", ChunkSize: 1},
	}

	for _, badCase := range badCases {
		err := ValidateMeasureExtractionConfig(&badCase)

		if err == nil {
			t.Error("expected an error, no error received")
		}
	}

	goodCases := []MeasureExtraction{
		{Database: "Database", Measure: "Measure", ChunkSize: 1},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, Limit: 1},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, From: "2019-01-01T00:00:00Z"},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, From: "2019-01-01T00:00:00+00:00"},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, From: "2019-01-01T00:00:00-01:00"},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, To: "2019-01-01T00:00:00-01:00"},
		{Database: "Database", Measure: "Measure", ChunkSize: 1, From: "2019-01-01T00:00:00-01:00", To: "2019-01-01T00:00:00+01:00"},
	}

	for _, goodCase := range goodCases {
		err := ValidateMeasureExtractionConfig(&goodCase)

		if err != nil {
			t.Errorf("expected not error, got: %v", err)
		}
	}
}
