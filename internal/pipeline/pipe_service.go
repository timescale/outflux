package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"

	"github.com/timescale/outflux/internal/connections"
)

const (
	pipeIDTemplate = "pipe_%d"
)

// PipeService defines methods for creating pipelines
type PipeService interface {
	Create(*ConnectionConfig, *MigrationConfig) []Pipe
}

type pipeService struct {
	tsConnService     connections.TSConnectionService
	influxConnService connections.InfluxConnectionService
	ingestorService   ingestion.IngestorService
	extractorService  extraction.ExtractorService
	extractionConfCreator
	ingestionConfCreator
}

func NewPipeService(
	tsConnService connections.TSConnectionService,
	influxConnService connections.InfluxConnectionService,
	ingestorService ingestion.IngestorService,
	extractorService extraction.ExtractorService) PipeService {
	return &pipeService{
		tsConnService:         tsConnService,
		influxConnService:     influxConnService,
		ingestorService:       ingestorService,
		extractorService:      extractorService,
		extractionConfCreator: &defaultExtractionConfCreator{},
		ingestionConfCreator:  &defaultIngestionConfCreator{},
	}
}

func (s *pipeService) Create(connConf *ConnectionConfig, conf *MigrationConfig) []Pipe {
	pipes := make([]Pipe, len(connConf.InputMeasures))
	for i, measure := range connConf.InputMeasures {
		pipeID := fmt.Sprintf(pipeIDTemplate, i)
		pipeConf := &PipeConfig{
			extraction:  s.extractionConfCreator.create(pipeID, connConf.InputDb, measure, conf),
			ingestion:   s.ingestionConfCreator.create(pipeID, conf),
			connections: connConf,
		}
		pipes[i] = &defPipe{
			id:               pipeID,
			tsConnService:    s.tsConnService,
			infConnService:   s.influxConnService,
			ingestorService:  s.ingestorService,
			extractorService: s.extractorService,
			conf:             pipeConf,
		}
	}

	return pipes
}
