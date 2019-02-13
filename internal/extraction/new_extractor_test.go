package extraction

import (
	"testing"

	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

func TestNewExtractor(t *testing.T) {
	tcs := []struct {
		desc      string
		expectErr bool
		conf      *config.Config
	}{
		{desc: "invalid measure extraction conf", expectErr: true, conf: invalidMeasureConf()},
		{desc: "connection params are nil", expectErr: true, conf: noConnConf()},
		{desc: "data set is nil", expectErr: true, conf: noDataSetConf()},
		{desc: "all is good", expectErr: true, conf: goodConf()},
	}

	for _, tc := range tcs {
		res, err := NewExtractor(tc.conf, &mockConnService{})
		if tc.expectErr && err == nil {
			t.Errorf("test case: %s\nexpected error, none received", tc.desc)
		}

		if !tc.expectErr && err != nil {
			t.Errorf("test case: %s\nno error expected. got: %v", err, tc.desc)
		}

		if tc.expectErr {
			return
		}

		castExtr := res.(*defaultInfluxExtractor)
		if castExtr.producer == nil {
			t.Error("producer in extractor was nil")
		}

		if castExtr.config != tc.conf {
			t.Errorf("expected config in extractor to be: %v\ngot: %v", tc.conf, castExtr.config)
		}
	}
}

func invalidMeasureConf() *config.Config {
	return &config.Config{MeasureExtraction: &config.MeasureExtraction{}}
}

func noConnConf() *config.Config {
	return &config.Config{MeasureExtraction: &config.MeasureExtraction{
		Database: "db", Measure: "m", ChunkSize: 1,
	}}
}

func noDataSetConf() *config.Config {
	return &config.Config{
		MeasureExtraction: &config.MeasureExtraction{Database: "db", Measure: "m", ChunkSize: 1},
		Connection:        &connections.InfluxConnectionParams{}}
}

func goodConf() *config.Config {
	return &config.Config{
		MeasureExtraction: &config.MeasureExtraction{Database: "db", Measure: "m", ChunkSize: 1},
		Connection:        &connections.InfluxConnectionParams{},
		DataSet:           &idrf.DataSetInfo{}}
}
