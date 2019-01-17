package config

import (
	influxUtils "github.com/timescale/outflux/schemadiscovery/clientutils"
)

// ExtractorConfig holds config properties for an Extractor that can connect to InfluxDB
type ExtractorConfig struct {
	Connection *influxUtils.ConnectionParams
	Measures   []*MeasureExtraction
}

// MeasureExtraction holds config properties for a single measure
type MeasureExtraction struct {
	Measure string
	From    string
	To      string
}
