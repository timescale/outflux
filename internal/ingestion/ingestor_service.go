package ingestion

import (
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/ingestion/ts"
	tsSchema "github.com/timescale/outflux/internal/schemamanagement/ts"
)

// IngestorService exposes methods to create new ingestors
type IngestorService interface {
	NewTimescaleIngestor(dbConn *pgx.Conn, config *config.IngestorConfig) Ingestor
}

// NewIngestorService creates an instance of the IngestorService
func NewIngestorService() IngestorService {
	return &ingestorService{}
}

type ingestorService struct {
}

// NewIngestor creates a new instance of an Ingestor with a specified config, for a specified
// data set and data channel
func (i *ingestorService) NewTimescaleIngestor(dbConn *pgx.Conn, config *config.IngestorConfig) Ingestor {
	schemaManager := tsSchema.NewTSSchemaManager(dbConn)
	return &ts.TSIngestor{
		DbConn:           dbConn,
		Config:           config,
		IngestionRoutine: ts.NewRoutine(),
		SchemaManager:    schemaManager,
	}
}
