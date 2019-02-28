package config

import (
	"fmt"

	"github.com/timescale/outflux/internal/schemamanagement"
)

// IngestorConfig holds all the properties required to create and run an ingestor
type IngestorConfig struct {
	IngestorID              string
	BatchSize               uint16
	RollbackOnExternalError bool
	CommitStrategy          CommitStrategy
	SchemaStrategy          schemamanagement.SchemaStrategy
}

// CommitStrategy describes how the ingestor should handle the ingested data
// Single Transaction or Commit on Each Batch
type CommitStrategy int

// Available values for the CommitStrategy enum
const (
	CommitOnEnd CommitStrategy = iota + 1
	CommitOnEachBatch
)

// ParseStrategyString returns the enum value matching the string, or an error
func ParseStrategyString(strategy string) (CommitStrategy, error) {
	switch strategy {
	case "CommitOnEnd":
		return CommitOnEnd, nil
	case "CommitOnEachBatch":
		return CommitOnEachBatch, nil
	default:
		return CommitOnEnd, fmt.Errorf("unknown commit strategy '%s'", strategy)
	}

}

func (s CommitStrategy) String() string {
	switch s {
	case CommitOnEnd:
		return "CommitOnEnd"
	case CommitOnEachBatch:
		return "CommitOnEachBatch"
	default:
		panic("unknown type")
	}
}
