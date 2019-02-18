package pipeline

import (
	"github.com/timescale/outflux/internal/connections"
	extractionConfig "github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/ingestion"
	"github.com/timescale/outflux/internal/schemamanagement/ts"
)

type ingestorCreator interface {
	create(pipeNum int, conf *MigrationConfig, extractionConf *extractionConfig.Config) (ingestion.Ingestor, error)
}

type defaultIngestorCreator struct {
	confCreator         ingestionConfCreator
	tsConnectionService connections.TSConnectionService
}

func newIngestorCreator(confCreator *defaultIngestionConfCreator, tsConnService connections.TSConnectionService) ingestorCreator {
	return &defaultIngestorCreator{confCreator, tsConnService}
}
func (s *defaultIngestorCreator) create(pipeNum int, conf *MigrationConfig, extractionConf *extractionConfig.Config) (ingestion.Ingestor, error) {
	in := s.confCreator.create(pipeNum, conf)
	connectionString := conf.Connection.OutputDbConnString
	dbConn, err := s.tsConnectionService.NewConnection(connectionString)
	if err != nil {
		return nil, err
	}

	schemaManager := ts.NewTSSchemaManager(dbConn)
	ingestor := ingestion.NewIngestor(in, schemaManager, dbConn, extractionConf.DataSet, extractionConf.DataChannel)
	return ingestor, nil
}
