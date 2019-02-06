package cmd

import (
	"fmt"
	"testing"

	ingestionConfig "github.com/timescale/outflux/ingestion/config"
	"github.com/timescale/outflux/pipeline"

	"github.com/timescale/outflux/integrationtestutils"
)

func TestMigrateSingleValue(t *testing.T) {
	// prepare influx db
	db := "test"
	measure := "test"
	cols := []string{"field1"}
	integrationtestutils.PrepareServersForITest(db)
	integrationtestutils.CreateInfluxMeasure(db, measure, cols)
	defer integrationtestutils.ClearServersAfterITest(db)

	config := defaultConfig(db, measure)
	errs := migrate(config)
	if errs != nil {
		panic(errs[0])
	}

	rows := integrationtestutils.ExecuteTsQuery(db, "SELECT * FROM "+measure)
	var time string
	var field1 int
	if !rows.Next() {
		panic("couldn't check state of TS DB")
	}

	err := rows.Scan(&time, &field1)
	if err != nil {
		panic("couldn't check state of TS DB")
	}

	if time == "" || field1 != 1 {
		panic(fmt.Sprintf("expected time != nil and field1=1\ngot: time %s, field1=%d", time, field1))
	}
	rows.Close()
}

func defaultConfig(db string, measure string) *pipeline.MigrationConfig {
	return &pipeline.MigrationConfig{
		InputHost:                            integrationtestutils.InfluxHost,
		InputDb:                              db,
		InputMeasures:                        []string{measure},
		OutputHost:                           integrationtestutils.TsHost,
		OutputDb:                             db,
		OutputDbSslMode:                      "disable",
		OutputUser:                           integrationtestutils.TsUser,
		OutputPassword:                       integrationtestutils.TsPass,
		OutputSchemaStrategy:                 ingestionConfig.CreateIfMissing,
		ChunkSize:                            1,
		Quiet:                                false,
		DataBuffer:                           1,
		MaxParallel:                          1,
		RollbackAllMeasureExtractionsOnError: true,
	}
}
