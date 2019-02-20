// +build integration

package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/testutils"
)

func TestSchemaTransfer(t *testing.T) {
	db := "test_schema_transfer"
	measure := "test"
	field := "field1"
	value := 1
	tags := make(map[string]string)
	fieldValues := make(map[string]interface{})
	fieldValues[field] = value
	testutils.PrepareServersForITest(t, db)
	testutils.CreateInfluxMeasure(t, db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	defer testutils.ClearServersAfterITest(t, db)

	config := &pipeline.SchemaTransferConfig{
		Connection: &pipeline.ConnectionConfig{
			InputHost:          testutils.InfluxHost,
			InputDb:            db,
			InputMeasures:      []string{measure},
			OutputDbConnString: fmt.Sprintf(testutils.TsConnStringTemplate, db),
		},
		OutputSchemaStrategy: schemamanagement.DropAndCreate,
	}
	appContext := initAppContext()
	_, err := transferSchema(appContext, config)
	if err != nil {
		t.Error(err)
	}

	dbConn := testutils.OpenTSConn(db)
	defer dbConn.Close()
	rows, err := dbConn.Query("SELECT count(*) FROM " + measure)
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()
	var count int
	if !rows.Next() {
		t.Error("couldn't check state of TS DB")
	}

	err = rows.Scan(&count)
	if err != nil {
		t.Error("couldn't check state of TS DB")
	}

	if count != 0 {
		t.Errorf("expected no rows in the output database, %d found", count)
	}
}

func TestOutputConnOverridesEnvVars(t *testing.T) {
	// Set up servers
	db := "test_output_con_overrides"
	measure := "test"
	field := "field1"
	value := 1
	tags := make(map[string]string)
	fieldValues := make(map[string]interface{})
	fieldValues[field] = value
	testutils.PrepareServersForITest(t, db)
	testutils.CreateInfluxMeasure(t, db, measure, []*map[string]string{&tags}, []*map[string]interface{}{&fieldValues})
	defer testutils.ClearServersAfterITest(t, db)

	// Three PG environment variables determening database and password
	os.Setenv("PGDATABASE", "wrong_db")
	os.Setenv("PGPORT", "5433")
	os.Setenv("PGPASSWORD", "postgres")

	// connection should fail, wrong db
	config := &pipeline.SchemaTransferConfig{
		Connection: &pipeline.ConnectionConfig{
			InputHost:     testutils.InfluxHost,
			InputDb:       db,
			InputMeasures: []string{measure},
		},
		OutputSchemaStrategy: schemamanagement.DropAndCreate,
	}
	appContext := initAppContext()
	_, err := transferSchema(appContext, config)
	if err == nil {
		t.Error("expected error, none received")
	}

	// Conn String that will override database and user
	connString := fmt.Sprintf("user=postgres dbname=%s", db)
	config.Connection.OutputDbConnString = connString
	_, err = transferSchema(appContext, config)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
