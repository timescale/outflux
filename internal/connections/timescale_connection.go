package connections

import (
	"log"

	"github.com/jackc/pgx"
)

// TSConnectionService creates new timescale db connections
type TSConnectionService interface {
	NewConnection(connectionString string) (*pgx.Conn, error)
}

type defaultTSConnectionService struct{}

// NewTSConnectionService creates a new TSConnectionService instance
func NewTSConnectionService() TSConnectionService {
	return &defaultTSConnectionService{}
}

func (s *defaultTSConnectionService) NewConnection(connectionString string) (*pgx.Conn, error) {
	log.Printf("Overriding PG environment variables for connection with: %s", connectionString)
	envConnConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, err
	}

	connConfig, err := pgx.ParseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	connConfig = envConnConfig.Merge(connConfig)
	return pgx.Connect(connConfig)
}
