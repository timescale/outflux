package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/connections"
	"github.com/timescale/outflux/idrf"
)

// PipelinesService defines methods for creating pipelines
type PipelinesService interface {
	CreatePipelines(dataSets []*idrf.DataSetInfo, conf *MigrationConfig) ([]ExecutionPipeline, error)
}

type defaultPipelinesService struct {
	tsConnectionService     connections.TSConnectionService
	influxConnectionService connections.InfluxConnectionService
	confCreator             pipelineConfCreator
}

func NewPipelinesService(
	tsConnService connections.TSConnectionService,
	influxConnService connections.InfluxConnectionService) PipelinesService {
	extractionConfCreator := &defaultExtractionConfCreator{}
	ingestConfCreator := &defaultIngestionConfCreator{}
	ingestorCreator := newIngestorCreator(ingestConfCreator, tsConnService)
	confCreator := newPipelineConfCreator(extractionConfCreator, ingestorCreator, influxConnService)
	return &defaultPipelinesService{tsConnService, influxConnService, confCreator}
}

func (s *defaultPipelinesService) CreatePipelines(dataSets []*idrf.DataSetInfo, conf *MigrationConfig) ([]ExecutionPipeline, error) {
	pipelines := make([]ExecutionPipeline, len(dataSets))
	for i, dataSet := range dataSets {
		pipeConf, err := s.confCreator.createPipelineConf(i, dataSet, conf)
		if err != nil {
			return nil, fmt.Errorf("error creating pipeline\n%v", err)
		}

		pipelines[i] = &defaultExecutionPipeline{
			id:     fmt.Sprintf("pipe_%d", i),
			config: pipeConf,
		}
	}
	return pipelines, nil
}
