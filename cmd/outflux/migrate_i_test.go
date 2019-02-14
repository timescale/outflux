// +build integration

package main

import (
	"github.com/timescale/outflux/internal/ingestion"
	"testing"

	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/testutils"
)

func TestMigrateSingleValue(t *testing.T) {
	// prepare influx db
	db := "test"
	measure := "test"
	field := "field1"
	value := 1
	tags := make(map[string]string)
	fieldValues := make(map[string]interface{})
	fieldValues[field] = value
	testutils.PrepareServersForITest(db)
	testutils.CreateInfluxMeasure(db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	defer testutils.ClearServersAfterITest(db)

	config := defaultConfig(db, measure)
	appContext := initAppContext()
	errs := migrate(appContext, config)
	if errs != nil {
		t.Error(errs[0])
	}

	rows := testutils.ExecuteTSQuery(db, "SELECT * FROM "+measure)
	var time string
	var field1 int
	if !rows.Next() {
		t.Error("couldn't check state of TS DB")
	}

	err := rows.Scan(&time, &field1)
	if err != nil {
		t.Error("couldn't check state of TS DB")
	}

	if time == "" || field1 != value {
		t.Errorf("expected time != nil and field1=%d\ngot: time %s, field1=%d", value, time, field1)
	}
	rows.Close()
}

func defaultConfig(db string, measure string) *pipeline.MigrationConfig {
	connConfig := &pipeline.ConnectionConfig{
		InputHost:       testutils.InfluxHost,
		InputDb:         db,
		InputMeasures:   []string{measure},
		OutputHost:      testutils.TsHost,
		OutputDb:        db,
		OutputDbSslMode: "disable",
		OutputUser:      testutils.TsUser,
		OutputPassword:  testutils.TsPass,
	}
	return &pipeline.MigrationConfig{
		Connection:                           connConfig,
		OutputSchemaStrategy:                 schemamanagement.CreateIfMissing,
		ChunkSize:                            1,
		Quiet:                                false,
		DataBuffer:                           1,
		MaxParallel:                          1,
		RollbackAllMeasureExtractionsOnError: true,
		BatchSize:                            1,
		CommitStrategy:                       ingestion.CommitOnEachBatch,
	}
}
