package connections

import (
	"log"
	"strings"

	"github.com/jackc/pgx"
)

// TSConnectionService creates new timescale db connections
type TSConnectionService interface {
	NewConnection(connectionString string) (PgxWrap, error)
}

type defaultTSConnectionService struct{}

// NewTSConnectionService creates a new TSConnectionService instance
func NewTSConnectionService() TSConnectionService {
	return &defaultTSConnectionService{}
}

func (s *defaultTSConnectionService) NewConnection(connectionString string) (PgxWrap, error) {
	log.Printf("Overriding PG environment variables for connection with: %s", connectionString)
	envConnConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(connectionString, `'`) && strings.HasSuffix(connectionString, `'`) {
		connectionString = connectionString[1 : len(connectionString)-1]
	} else if strings.HasPrefix(connectionString, `"`) && strings.HasSuffix(connectionString, `"`) {
		connectionString = connectionString[1 : len(connectionString)-1]
	}

	connConfig, err := pgx.ParseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	connConfig = envConnConfig.Merge(connConfig)
	pgxConn, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, err
	}

	return NewPgxWrapper(pgxConn), nil
}
