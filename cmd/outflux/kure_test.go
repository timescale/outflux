package main

import (
	"testing"

	"github.com/timescale/outflux/internal/schemamanagement"

	"github.com/timescale/outflux/internal/pipeline"
)

func TestKurence(t *testing.T) {
	appContext := initAppContext()

	connArgs := &pipeline.ConnectionConfig{
		InputHost:          "http://localhost:8086",
		InputDb:            "benchmark",
		InputMeasures:      []string{"cpu"},
		InputUser:          "test",
		InputPass:          "test",
		OutputDbConnString: "dbname=benchmark sslmode=disable",
	}

	args := &pipeline.MigrationConfig{
		OutputSchemaStrategy: schemamanagement.DropAndCreate,
		Limit:                1,
		DataBuffer:           1,
		MaxParallel:          1,
		ChunkSize:            1,
	}
	migrate(appContext, connArgs, args)
}
