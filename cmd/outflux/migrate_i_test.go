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
	db := "test_single_value"
	measure := "test"
	field := "field1"
	value := 1
	tags := make(map[string]string)
	fieldValues := make(map[string]interface{})
	fieldValues[field] = value
	if err := testutils.PrepareServersForITest(db); err != nil {
		t.Fatalf("could not prepare servers: %v", err)
	}
	err := testutils.CreateInfluxMeasure(db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	if err != nil {
		t.Fatalf("could not prepare influx measurement: %v", err)
	}

	defer testutils.ClearServersAfterITest(db)

	// run
	connConf, config := defaultConfig(db, measure)
	appContext := initAppContext()
	err = migrate(appContext, connConf, config)
	if err != nil {
		t.Fatal(err)
	}

	// check
	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()
	rows, err := dbConn.Query("SELECT * FROM " + measure)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var time time.Time
	var field1 int
	if !rows.Next() {
		t.Fatal("couldn't check state of TS DB")
	}

	err = rows.Scan(&time, &field1)
	if err != nil {
		t.Fatal("couldn't check state of TS DB")
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
	db := "test_tags_json"
	measure := "test"
	tag := "tag1"
	field := "field1"
	value := 1
	tagValue := "1"
	tags := map[string]string{tag: tagValue}
	fieldValues := map[string]interface{}{field: value}
	if err := testutils.DeleteTimescaleDb(db); err != nil {
		t.Fatalf("could not delete if exists ts db: %v", err)
	}

	if err := testutils.PrepareServersForITest(db); err != nil {
		t.Fatalf("could not prepare servers: %v", err)
	}
	defer testutils.ClearServersAfterITest(db)

	err := testutils.CreateInfluxMeasure(db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	if err != nil {
		t.Fatal(err)
	}

	// run
	connConf, config := defaultConfig(db, measure)
	config.TagsAsJSON = true
	config.TagsCol = "tags"
	appContext := initAppContext()
	err = migrate(appContext, connConf, config)
	if err != nil {
		t.Fatal(err)
	}

	// check
	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatalf("could not open db conn: %v", err)
	}
	defer dbConn.Close()

	rows, err := dbConn.Query("SELECT * FROM " + measure)
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()
	var time time.Time
	var field1 int
	var tagsCol string
	if !rows.Next() {
		t.Fatal("couldn't check state of TS DB")
	}

	err = rows.Scan(&time, &tagsCol, &field1)
	if err != nil {
		t.Fatal("couldn't check state of TS DB")
	}

	if time.Before(start) || field1 != value || tagsCol != "{\"tag1\": \"1\"}" {
		t.Errorf("expected time > %v and field1=%d and tags={\"tag1\": \"1\"}\ngot: time %s, field1=%d, tags=%s", start, value, time, field1, tagsCol)
	}
}

func TestMigrateFieldsAsJson(t *testing.T) {
	//prepare influx db
	start := time.Now().UTC()
	db := "test_fields_json"
	measure := "test"
	field := "field1"
	value := 1
	tags := map[string]string{}
	fieldValues := map[string]interface{}{field: value}
	if err := testutils.PrepareServersForITest(db); err != nil {
		t.Fatalf("could not prepare servers: %v", err)
	}

	err := testutils.CreateInfluxMeasure(db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	if err != nil {
		t.Fatalf("could not prepare servers: %v", err)
	}

	defer testutils.ClearServersAfterITest(db)

	// run
	connConf, config := defaultConfig(db, measure)
	config.FieldsAsJSON = true
	config.FieldsCol = "fields"
	appContext := initAppContext()
	errs := migrate(appContext, connConf, config)
	if errs != nil {
		t.Fatal(errs)
	}

	// check
	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()

	rows, err := dbConn.Query("SELECT * FROM " + measure)
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()
	var time time.Time
	var fieldsCol string
	if !rows.Next() {
		t.Fatal("couldn't check state of TS DB")
	}

	err = rows.Scan(&time, &fieldsCol)
	if err != nil {
		t.Fatal("couldn't check state of TS DB")
	}

	if time.Before(start) || fieldsCol != "{\"field1\": 1}" {
		t.Errorf("expected time > %v and fields={\"field1\": 1}\ngot: time %s, field1=%s", start, time, fieldsCol)
	}
}
