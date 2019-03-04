package ingestion

import (
	"github.com/timescale/outflux/internal/idrf"
)

const (
	// example: postgres://test:test@localhost:5432/test?sslmode=disable
	postgresConnectionStringTemplate = "postgres://%s:%s@%s/%s%s"
)

// Ingestor takes a data channel of idrf rows and inserts them in a target database
type Ingestor interface {
	ID() string
	Prepare(conn *idrf.Bundle) error
	Start(chan error) error
}
