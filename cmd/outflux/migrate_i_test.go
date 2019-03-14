// +build integration

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/timescale/outflux/internal/cli"
	ingestionConfig "github.com/timescale/outflux/internal/ingestion/config"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"

	"github.com/timescale/outflux/internal/testutils"
)

func TestMigrateSingleValue(t *testing.T) {
	// prepare influx db
	start := time.Now().UTC()
	db := "test"
	measure := "test"
	field := "field1"
	value := 1
	tags := make(map[string]string)
	fieldValues := make(map[string]interface{})
	fieldValues[field] = value
	testutils.PrepareServersForITest(t, db)
	testutils.CreateInfluxMeasure(t, db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	defer testutils.ClearServersAfterITest(t, db)

	// run
	connConf, config := defaultConfig(db, measure)
	appContext := initAppContext()
	errs := migrate(appContext, connConf, config)
	if errs != nil {
		t.Error(errs[0])
		return
	}

	// check
	dbConn := testutils.OpenTSConn(db)
	defer dbConn.Close()
	rows, err := dbConn.Query("SELECT * FROM " + measure)
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	var time time.Time
	var field1 int
	if !rows.Next() {
		t.Fatal("couldn't check state of TS DB")
		return
	}

	err = rows.Scan(&time, &field1)
	if err != nil {
		t.Fatal("couldn't check state of TS DB")
		return
	}

	if time.Before(start) || field1 != value {
		t.Errorf("expected time > %v and field1=%d\ngot: time %s, field1=%d", start, value, time, field1)
	}
}

func defaultConfig(db string, measure string) (*cli.ConnectionConfig, *cli.MigrationConfig) {
	connConfig := &cli.ConnectionConfig{
		InputHost:          testutils.InfluxHost,
		InputDb:            db,
		InputMeasures:      []string{measure},
		OutputDbConnString: fmt.Sprintf(testutils.TsConnStringTemplate, db),
	}
	return connConfig, &cli.MigrationConfig{
		OutputSchemaStrategy:                 schemaconfig.CreateIfMissing,
		ChunkSize:                            1,
		DataBuffer:                           1,
		MaxParallel:                          1,
		RollbackAllMeasureExtractionsOnError: true,
		BatchSize:                            1,
		CommitStrategy:                       ingestionConfig.CommitOnEachBatch,
		Quiet:                                true,
	}
}

func TestMigrateTagsAsJson(t *testing.T) {
	//prepare influx db
	start := time.Now().UTC()
	db := "test"
	measure := "test"
	tag := "tag1"
	field := "field1"
	value := 1
	tagValue := "1"
	tags := map[string]string{tag: tagValue}
	fieldValues := map[string]interface{}{field: value}
	testutils.PrepareServersForITest(t, db)
	testutils.CreateInfluxMeasure(t, db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	defer testutils.ClearServersAfterITest(t, db)

	// run
	connConf, config := defaultConfig(db, measure)
	config.TagsAsJSON = true
	config.TagsCol = "tags"
	appContext := initAppContext()
	errs := migrate(appContext, connConf, config)
	if errs != nil {
		t.Error(errs[0])
		return
	}

	// check
	dbConn := testutils.OpenTSConn(db)
	defer dbConn.Close()

	rows, err := dbConn.Query("SELECT time, tags, field1 FROM " + measure)
	if err != nil {
		t.Error(err)
		return
	}

	defer rows.Close()
	var time time.Time
	var field1 int
	var tagsCol string
	if !rows.Next() {
		t.Fatal("couldn't check state of TS DB")
		return
	}

	err = rows.Scan(&time, &tagsCol, &field1)
	if err != nil {
		t.Fatal("couldn't check state of TS DB")
		return
	}

	if time.Before(start) || field1 != value || tagsCol != "{\"tag1\": \"1\"}" {
		t.Errorf("expected time > %v and field1=%d\ngot: time %s, field1=%d", start, value, time, field1)
	}
}
