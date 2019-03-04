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
	db := "test"
	testutils.CreateTimescaleDb(t, db)
	defer testutils.DeleteTimescaleDb(t, db)
	creator := &defaultTableCreator{}
	dbConn := testutils.OpenTSConn(db)
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: "name",
		Columns: []*idrf.ColumnInfo{
			&idrf.ColumnInfo{Name: "col1", DataType: idrf.IDRFTimestamptz},
			&idrf.ColumnInfo{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	err := creator.CreateTable(dbConn, dataSet)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
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
			t.Error(err)
		}

		if colInfo.Name != name || colInfo.DataType != pgTypeToIdrf(dataType) {
			t.Errorf("Expected column name: %s and type %v\ngot: %s and %s", colInfo.Name, colInfo.DataType, name, dataType)
		}
		currCol++
	}
	if currCol == 0 {
		t.Error("table wasn't created")
	}

	// Creating the table again should fail
	err = creator.CreateTable(dbConn, dataSet)
	if err == nil {
		t.Error("table creation should have failed because table exists")
	}
}
