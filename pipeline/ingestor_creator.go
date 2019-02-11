package pipeline

import (
	"github.com/timescale/outflux/connections"
	extractionConfig "github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/ingestion"
	"github.com/timescale/outflux/schemamanagement/ts"
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
	connectionParams := migrationToTSConnectionParams(conf.Connection)
	dbConn, err := s.tsConnectionService.NewConnection(connectionParams)
	if err != nil {
		return nil, err
	}

	schemaManager := ts.NewTSSchemaManager(dbConn)
	ingestor := ingestion.NewIngestor(in, schemaManager, dbConn, extractionConf.DataSet, extractionConf.DataChannel)
	return ingestor, nil
}

func migrationToTSConnectionParams(conf *ConnectionConfig) *connections.TSConnectionParams {
	additionalConnParams := make(map[string]string)
	additionalConnParams["sslmode"] = conf.OutputDbSslMode
	return &connections.TSConnectionParams{
		Server:               conf.OutputHost,
		Username:             conf.OutputUser,
		Password:             conf.OutputPassword,
		Database:             conf.OutputDb,
		AdditionalConnParams: additionalConnParams,
	}
}
