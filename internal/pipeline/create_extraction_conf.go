package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/connections"
	extractionConfig "github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

type extractionConfCreator interface {
	createExtractionConf(pipeNum int, conf *MigrationConfig, dataSet *idrf.DataSetInfo) *extractionConfig.Config
}

type defaultExtractionConfCreator struct{}

func (d *defaultExtractionConfCreator) createExtractionConf(pipeNum int, conf *MigrationConfig, dataSet *idrf.DataSetInfo) *extractionConfig.Config {
	measureExtractionConf := &extractionConfig.MeasureExtraction{
		Database:  conf.Connection.InputDb,
		Measure:   dataSet.DataSetName,
		From:      conf.From,
		To:        conf.To,
		ChunkSize: conf.ChunkSize,
		Limit:     conf.Limit,
	}
	connection := &connections.InfluxConnectionParams{
		Server:   conf.Connection.InputHost,
		Username: conf.Connection.InputUser,
		Password: conf.Connection.InputPass,
	}

	ex := &extractionConfig.Config{
		ExtractorID:       fmt.Sprintf("pipe_%d_ext", pipeNum),
		MeasureExtraction: measureExtractionConf,
		Connection:        connection,
		DataSet:           dataSet,
		DataChannel:       make(chan idrf.Row, conf.DataBuffer),
	}

	return ex
}
