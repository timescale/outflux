package connections

import (
	"github.com/jackc/pgx"
	// Postgres driver
	_ "github.com/lib/pq"
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
	connConfig, err := pgx.ParseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	return pgx.Connect(connConfig)
}
