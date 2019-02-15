package connections

import (
	"github.com/jackc/pgx"
	"log"

	// Postgres driver
	_ "github.com/lib/pq"
)

// TSConnectionService creates new timescale db connections
type TSConnectionService interface {
	NewConnection(connectionString string) (*pgx.Conn, error)
	NewConnectionFromEnvVars() (*pgx.Conn, error)
}

type defaultTSConnectionService struct{}

// NewTSConnectionService creates a new TSConnectionService instance
func NewTSConnectionService() TSConnectionService {
	return &defaultTSConnectionService{}
}

func (s *defaultTSConnectionService) NewConnection(connectionString string) (*pgx.Conn, error) {
	log.Printf("Attempting TimescaleDB connection with: %s", connectionString)
	connConfig, err := pgx.ParseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	return pgx.Connect(connConfig)
}

func (s *defaultTSConnectionService) NewConnectionFromEnvVars() (*pgx.Conn, error) {
	log.Printf("Attempting Timescale connection with environment variables")
	connConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, err
	}

	return pgx.Connect(connConfig)
}
