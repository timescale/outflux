// +build integration

package main

import (
	"testing"

	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/testutils"
)

func TestSchemaTransfer(t *testing.T) {
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

	config := &pipeline.SchemaTransferConfig{
		Connection: &pipeline.ConnectionConfig{
			InputHost:       testutils.InfluxHost,
			InputDb:         db,
			InputMeasures:   []string{measure},
			OutputHost:      testutils.TsHost,
			OutputDb:        db,
			OutputDbSslMode: "disable",
			OutputUser:      testutils.TsUser,
			OutputPassword:  testutils.TsPass,
		},
		OutputSchemaStrategy: schemamanagement.DropAndCreate,
	}
	appContext := initAppContext()
	_, err := transferSchema(appContext, config)
	if err != nil {
		t.Error(err)
	}

	rows := testutils.ExecuteTSQuery(db, "SELECT count(*) FROM "+measure)
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
