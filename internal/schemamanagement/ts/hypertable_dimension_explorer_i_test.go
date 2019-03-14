// +build integration

package ts

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/internal/testutils"
)

func TestIsTimePartitionedBy(t *testing.T) {
	db := "test_time_partitioned_by"
	if err := testutils.DeleteTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}

	if err := testutils.CreateTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}

	defer testutils.DeleteTimescaleDb(db)

	checker := defaultHypertableDimensionExplorer{}

	notHypertable := "not_hypertable"
	wrongPartitionType := "partitioned_by_int"
	wrongPartitioningCol := "partitioned_by_other_name"
	wrongCol := "wrong_column"
	okTable := "good_hypertable"
	okCol := "ok_column"

	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()

	createOkTableQuery := fmt.Sprintf("CREATE TABLE %s (%s TIMESTAMPTZ NOT NULL)", okTable, okCol)
	createOkHypertable := fmt.Sprintf("SELECT create_hypertable('%s','%s')", okTable, okCol)
	createNotHypertable := fmt.Sprintf("CREATE TABLE %s(%s int)", notHypertable, okCol)
	createWrongPartColumnType := fmt.Sprintf("CREATE TABLE %s(%s INTEGER NOT NULL)", wrongPartitionType, okCol)
	createWrongPartColHypertable := fmt.Sprintf("SELECT create_hypertable('%s','%s', chunk_time_interval => 100)", wrongPartitionType, okCol)
	createWrongPartColumnName := fmt.Sprintf("CREATE TABLE %s(%s TIMESTAMP NOT NULL)", wrongPartitioningCol, wrongCol)
	createWrongPartColNameHypertable := fmt.Sprintf("SELECT create_hypertable('%s', '%s')", wrongPartitioningCol, wrongCol)

	dbConn.Exec(createOkTableQuery)
	dbConn.Exec(createOkHypertable)
	dbConn.Exec(createNotHypertable)
	dbConn.Exec(createWrongPartColumnType)
	dbConn.Exec(createWrongPartColHypertable)
	dbConn.Exec(createWrongPartColumnName)
	dbConn.Exec(createWrongPartColNameHypertable)
	tcs := []struct {
		table     string
		timeCol   string
		expectRes bool
	}{
		{table: notHypertable},
		{table: wrongPartitionType},
		{table: wrongPartitioningCol, timeCol: okCol},
		{table: okTable, timeCol: okCol, expectRes: true},
	}

	for _, tc := range tcs {
		res, err := checker.isTimePartitionedBy(dbConn, "", tc.table, tc.timeCol)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != tc.expectRes {
			t.Fatalf("expected %v, got %v", tc.expectRes, res)
		}
	}
}
