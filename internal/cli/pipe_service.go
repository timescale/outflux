package cli

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/pipeline"
)

const (
	pipeIDTemplate = "pipe_%s"
)

// PipeService defines methods for creating pipelines
type PipeService interface {
	Create(infConn influx.Client, pgConn *pgx.Conn, measure, inputDb string, conf *MigrationConfig) (pipeline.Pipe, error)
}

type pipeService struct {
	ingestorService    ingestion.IngestorService
	extractorService   extraction.ExtractorService
	transformerService TransformerService
	extractionConfCreator
	ingestionConfCreator
}

// NewPipeService creates a new instance of the PipeService
func NewPipeService(
	ingestorService ingestion.IngestorService,
	extractorService extraction.ExtractorService,
	transformerService TransformerService) PipeService {
	return &pipeService{
		ingestorService:       ingestorService,
		extractorService:      extractorService,
		transformerService:    transformerService,
		extractionConfCreator: &defaultExtractionConfCreator{},
		ingestionConfCreator:  &defaultIngestionConfCreator{},
	}
}

func (s *pipeService) Create(infConn influx.Client, tsConn *pgx.Conn, measure, inputDb string, conf *MigrationConfig) (pipeline.Pipe, error) {
	pipeID := fmt.Sprintf(pipeIDTemplate, measure)
	extractionConf := s.extractionConfCreator.create(pipeID, inputDb, measure, conf)
	ingestionConf := s.ingestionConfCreator.create(pipeID, conf)
	extractor, ingestor, err := s.createElements(infConn, tsConn, extractionConf, ingestionConf)
	if err != nil {
		return nil, fmt.Errorf("%s: could not create extractor and ingestor:\n%v", pipeID, err)
	}

	transformers, err := s.createTransformers(pipeID, infConn, measure, inputDb, conf)
	if err != nil {
		return nil, fmt.Errorf("%s: could not create transformers:\n%v", pipeID, err)
	}

	return pipeline.NewPipe(pipeID, ingestor, extractor, transformers, conf.SchemaOnly), nil
}
