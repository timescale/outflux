package pipeline

import (
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/idrf"
)

type pipelineConfCreator interface {
	createPipelineConf(pipeNum int, dataSet *idrf.DataSetInfo, conf *MigrationConfig) (*PipelineConfig, error)
}

type defaultPipeConfCreator struct {
	extraction              extractionConfCreator
	ingestion               ingestorCreator
	influxConnectionService connections.InfluxConnectionService
}

func newPipelineConfCreator(
	extraction extractionConfCreator,
	ingestion ingestorCreator,
	influxConnService connections.InfluxConnectionService) pipelineConfCreator {
	return &defaultPipeConfCreator{extraction, ingestion, influxConnService}
}

func (s *defaultPipeConfCreator) createPipelineConf(pipeNum int, dataSet *idrf.DataSetInfo, conf *MigrationConfig) (*PipelineConfig, error) {
	extractionConfig := s.extraction.createExtractionConf(pipeNum, conf, dataSet)
	extractor, err := extraction.NewExtractor(extractionConfig, s.influxConnectionService)
	if err != nil {
		return nil, err
	}

	ingestor, err := s.ingestion.create(pipeNum, conf, extractionConfig)
	return &PipelineConfig{
		Extractor: extractor,
		Ingestor:  ingestor,
	}, nil
}
