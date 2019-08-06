// +build integration

package ts

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/testutils"
)

func TestIntegratedCreateTable(t *testing.T) {
	db := "test_create_table"
	require.NoError(t, testutils.DeleteTimescaleDb(db))
	require.NoError(t, testutils.CreateTimescaleDb(db))
	defer testutils.DeleteTimescaleDb(db)
	creator := &defaultTableCreator{}
	dbConn, err := testutils.OpenTSConn(db)
	require.NoError(t, err)
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: "name",
		Columns: []*idrf.Column{
			{Name: "col1", DataType: idrf.IDRFTimestamptz},
			{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	require.NoError(t, creator.CreateTable(dbConn, dataSet))

	tableColumns := fmt.Sprintf(`SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s`, "'public'", "'name'")
	rows, err := dbConn.Query(tableColumns)
	assert.NoError(t, err)
	defer rows.Close()
	currCol := 0
	for rows.Next() {
		var name, dataType string
		colInfo := dataSet.Columns[currCol]
		require.NoError(t, rows.Scan(&name, &dataType))
		require.Equal(t, colInfo.Name, name)
		require.Equal(t, colInfo.DataType, pgTypeToIdrf(dataType))
		currCol++
	}
	require.NotZero(t, currCol)
	// Creating the table again should fail
	err = creator.CreateTable(dbConn, dataSet)
	assert.Error(t, err)
}

func TestCreateTableWithSchema(t *testing.T) {
	db := "test_create_table_with_schema"
	targetSchema := "some_schema"
	require.NoError(t, testutils.DeleteTimescaleDb(db))
	require.NoError(t, testutils.CreateTimescaleDb(db))
	defer testutils.DeleteTimescaleDb(db)
	require.NoError(t, testutils.CreateTimescaleSchema(db, targetSchema))

	dbConn, err := testutils.OpenTSConn(db)
	require.NoError(t, err)
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: "name",
		Columns: []*idrf.Column{
			{Name: "col1", DataType: idrf.IDRFTimestamptz},
			{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	creator := &defaultTableCreator{
		schema: targetSchema,
	}
	require.NoError(t, creator.CreateTable(dbConn, dataSet))

	tableColumns := fmt.Sprintf(`SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s`, "'"+targetSchema+"'", "'name'")
	rows, err := dbConn.Query(tableColumns)
	require.NoError(t, err)
	defer rows.Close()
	currCol := 0
	for rows.Next() {
		var name, dataType string
		colInfo := dataSet.Columns[currCol]
		require.NoError(t, rows.Scan(&name, &dataType))
		require.Equal(t, colInfo.Name, name)
		require.Equal(t, colInfo.DataType, pgTypeToIdrf(dataType))
		currCol++
	}
	require.NotZero(t, currCol)
}

func TestIntegratedUpdateMetadata(t *testing.T) {
	db := "test_update_metadata"
	require.NoError(t, testutils.DeleteTimescaleDb(db))
	require.NoError(t, testutils.CreateTimescaleDb(db))
	defer testutils.DeleteTimescaleDb(db)
	explorer := &defaultTableFinder{}
	creator := &defaultTableCreator{}
	dbConn, err := testutils.OpenTSConn(db)
	require.NoError(t, err)
	defer dbConn.Close()
	dbConn.Exec(createTimescaleExtensionQuery)
	metadataTableName, err := explorer.metadataTableName(dbConn)
	require.NoError(t, err)

	if metadataTableName == "" {
		return
	}

	dbConn.Exec(fmt.Sprintf("DELETE FROM %s.%s WHERE key='%s'", timescaleCatalogSchema,
		metadataTableName,
		metadataKey))
	timeBeforeUpdate := time.Now()
	time.Sleep(1 * time.Second)
	require.NoError(t, creator.UpdateMetadata(dbConn, metadataTableName))

	q := fmt.Sprintf("SELECT value FROM %s.%s WHERE key='%s'",
		timescaleCatalogSchema,
		metadataTableName,
		metadataKey)
	rows, err := dbConn.Query(q)
	require.True(t, err == nil && rows.Next())

	var updateTimeValStr string
	require.NoError(t, rows.Scan(&updateTimeValStr))
	rows.Close()

	updateTimeVal, _ := time.Parse(time.RFC3339, updateTimeValStr)
	require.True(t, updateTimeVal.After(timeBeforeUpdate))

	// update again, first time it inserts, second time it updates the same key
	time.Sleep(1 * time.Second)
	err = creator.UpdateMetadata(dbConn, metadataTableName)
	rows2, err := dbConn.Query(q)
	require.True(t, err == nil && rows2.Next())
	defer rows2.Close()
	require.NoError(t, rows2.Scan(&updateTimeValStr))
	updateTimeVal2, _ := time.Parse(time.RFC3339, updateTimeValStr)
	require.True(t, updateTimeVal2.After(updateTimeVal))
}

func TestMetadataTableNameNoPermissions(t *testing.T) {
	db := "test_update_metadata_2"
	require.NoError(t, testutils.DeleteTimescaleDb(db))
	require.NoError(t, testutils.CreateTimescaleDb(db))
	defer testutils.DeleteTimescaleDb(db)
	require.NoError(t, testutils.CreateNonAdminInTS("dumb", "dumber"))

	explorer := &defaultTableFinder{}
	dbConnAdmin, err := testutils.OpenTSConn(db)
	require.NoError(t, err)
	dbConnAdmin.Exec(createTimescaleExtensionQuery)
	dbConnAdmin.Close()

	dbConn, err := testutils.OpenTsConnWithUser(db, "dumb", "dumber")
	require.NoError(t, err)
	defer dbConn.Close()
	metadataTable, err := explorer.metadataTableName(dbConn)
	require.NoError(t, err)

	creator := defaultTableCreator{}
	require.Error(t, creator.UpdateMetadata(dbConn, metadataTable))
}

func TestCreateTableWithCustomChunkInterval(t *testing.T) {
	db := "test_create_table_with_custom_chunk_size"
	require.NoError(t, testutils.DeleteTimescaleDb(db), "could not prepare db")
	require.NoError(t, testutils.CreateTimescaleDb(db), "could not prepare db")
	defer testutils.DeleteTimescaleDb(db)
	creator := &defaultTableCreator{chunkTimeInterval: "1m"}
	dbConn, err := testutils.OpenTSConn(db)
	require.NoError(t, err)
	defer dbConn.Close()
	dataSet := &idrf.DataSet{
		DataSetName: "name",
		Columns: []*idrf.Column{
			{Name: "col1", DataType: idrf.IDRFTimestamptz},
			{Name: "col2", DataType: idrf.IDRFInteger64},
		},
		TimeColumn: "col1",
	}
	err = creator.CreateTable(dbConn, dataSet)
	require.NoError(t, err)

	expectedChunkInterval := int64(60000000)
	getHypertableID := `SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name='` + dataSet.DataSetName + `'`
	rows, err := dbConn.Query(getHypertableID)
	require.NoError(t, err)

	var hypertableID int
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&hypertableID))
	require.NotZero(t, hypertableID)
	rows.Close()

	getChunkInterval := `SELECT interval_length FROM _timescaledb_catalog.dimension WHERE hypertable_id=` + strconv.Itoa(hypertableID)
	rows2, err := dbConn.Query(getChunkInterval)
	assert.NoError(t, err)
	defer rows2.Close()
	require.True(t, rows2.Next())
	var chunkInterval int64
	require.NoError(t, rows2.Scan(&chunkInterval))
	require.Equal(t, expectedChunkInterval, chunkInterval)
}
