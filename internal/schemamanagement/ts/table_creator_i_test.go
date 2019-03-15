// +build integration

package ts

import (
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/testutils"
)

func TestCreateTable(t *testing.T) {
	db := "test_create_table"
	if err := testutils.DeleteTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}
	if err := testutils.CreateTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}
	defer testutils.DeleteTimescaleDb(db)
	creator := &defaultTableCreator{}
	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: "name",
		Columns: []*idrf.Column{
			&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamptz},
			&idrf.Column{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	err = creator.CreateTable(dbConn, dataSet)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	tableColumns := fmt.Sprintf(`SELECT column_name, data_type
	FROM information_schema.columns
	WHERE table_schema = %s AND table_name = %s`, "'public'", "'name'")
	rows, err := dbConn.Query(tableColumns)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	currCol := 0
	for rows.Next() {
		var name, dataType string
		colInfo := dataSet.Columns[currCol]
		err = rows.Scan(&name, &dataType)
		if err != nil {
			t.Fatal(err)
		}

		if colInfo.Name != name || colInfo.DataType != pgTypeToIdrf(dataType) {
			t.Fatalf("Expected column name: %s and type %v\ngot: %s and %s", colInfo.Name, colInfo.DataType, name, dataType)
		}
		currCol++
	}
	if currCol == 0 {
		t.Fatal("table wasn't created")
	}

	// Creating the table again should fail
	err = creator.CreateTable(dbConn, dataSet)
	if err == nil {
		t.Error("table creation should have failed because table exists")
	}
}

func TestCreateTableWithSchema(t *testing.T) {
	db := "test_create_table_with_schema"
	targetSchema := "some_schema"
	if err := testutils.DeleteTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}
	if err := testutils.CreateTimescaleDb(db); err != nil {
		t.Fatalf("could not prepare db: %v", err)
	}
	defer testutils.DeleteTimescaleDb(db)
	if err := testutils.CreateTimescaleSchema(db, targetSchema); err != nil {
		t.Fatalf("could not create target schema: %v", err)
	}

	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: targetSchema + ".name",
		Columns: []*idrf.Column{
			&idrf.Column{Name: "col1", DataType: idrf.IDRFTimestamptz},
			&idrf.Column{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	creator := &defaultTableCreator{}
	if err := creator.CreateTable(dbConn, dataSet); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	tableColumns := fmt.Sprintf(`SELECT column_name, data_type
	FROM information_schema.columns
	WHERE table_schema = %s AND table_name = %s`, "'"+targetSchema+"'", "'name'")
	rows, err := dbConn.Query(tableColumns)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	currCol := 0
	for rows.Next() {
		var name, dataType string
		colInfo := dataSet.Columns[currCol]
		err = rows.Scan(&name, &dataType)
		if err != nil {
			t.Fatal(err)
		}
		if colInfo.Name != name || colInfo.DataType != pgTypeToIdrf(dataType) {
			t.Fatalf("Expected column name: %s and type %v\ngot: %s and %s", colInfo.Name, colInfo.DataType, name, dataType)
		}
		currCol++
	}
	if currCol == 0 {
		t.Fatal("table wasn't created")
	}
}
