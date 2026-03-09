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

// TestMigrateAdoptExistingWithTimeColumnRename tests our two new features together:
//   1. --time-column: rename 'time' -> 'created_at'
//   2. AdoptExisting schema strategy: product_id tag (TEXT) -> existing INTEGER column
//
// This mirrors the real price_history scenario where:
//   - InfluxDB has: time, product_id (tag), symbol (tag), mark_price (float field)
//   - TimescaleDB has: created_at (timestamp), product_id (integer), symbol (text), mark_price (double)
func TestMigrateAdoptExistingWithTimeColumnRename(t *testing.T) {
	db := "test_adopt_existing"
	measure := "price_history"

	// Cleanup any leftover state
	_ = testutils.DeleteTimescaleDb(db)
	_ = testutils.DeleteInfluxDb(db)

	// Create databases
	if err := testutils.PrepareServersForITest(db); err != nil {
		t.Fatalf("could not prepare servers: %v", err)
	}
	defer testutils.ClearServersAfterITest(db)

	// Capture start time before writing to InfluxDB so the stored timestamp is >= start
	start := time.Now().UTC()

	// Insert a data point in InfluxDB:
	//   tags:   product_id="42", symbol="BTC"
	//   fields: mark_price=50000.5
	tags := map[string]string{
		"product_id": "42",
		"symbol":     "BTC",
	}
	fields := map[string]interface{}{
		"mark_price": 50000.5,
	}
	if err := testutils.CreateInfluxMeasure(db, measure,
		[]*map[string]string{&tags},
		[]*map[string]interface{}{&fields},
	); err != nil {
		t.Fatalf("could not insert influx data: %v", err)
	}

	// Create the pre-existing TimescaleDB hypertable with the "mismatched" schema:
	//   - time column is 'created_at' (not 'time')
	//   - product_id is INTEGER (not TEXT)
	tsConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatalf("could not open ts connection: %v", err)
	}
	createTableSQL := `
		CREATE EXTENSION IF NOT EXISTS timescaledb;
		CREATE TABLE price_history (
			created_at  TIMESTAMP    NOT NULL,
			product_id  INTEGER,
			symbol      TEXT,
			mark_price  DOUBLE PRECISION
		);
		SELECT create_hypertable('price_history', 'created_at');
	`
	if _, err = tsConn.Exec(createTableSQL); err != nil {
		tsConn.Close()
		t.Fatalf("could not create timescale table: %v", err)
	}
	tsConn.Close()

	// Run the migration using our two new features
	connConf := &cli.ConnectionConfig{
		InputHost:          testutils.InfluxHost,
		InputDb:            db,
		InputMeasures:      []string{measure},
		OutputDbConnString: fmt.Sprintf(testutils.TsConnStringTemplate, db),
	}
	migrateConf := &cli.MigrationConfig{
		OutputSchemaStrategy:                 schemaconfig.AdoptExisting, // skip validation, adopt TS types
		TimeColumn:                           "created_at",               // rename time -> created_at
		ChunkSize:                            10,
		DataBuffer:                           10,
		MaxParallel:                          1,
		RollbackAllMeasureExtractionsOnError: true,
		BatchSize:                            10,
		CommitStrategy:                       ingestionConfig.CommitOnEachBatch,
		Quiet:                                true,
	}

	appCtx := initAppContext()
	if err = migrate(appCtx, connConf, migrateConf); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// Verify the data landed in TimescaleDB correctly
	dbConn, err := testutils.OpenTSConn(db)
	if err != nil {
		t.Fatalf("could not open ts connection for verification: %v", err)
	}
	defer dbConn.Close()

	rows, err := dbConn.Query("SELECT created_at, product_id, symbol, mark_price FROM price_history")
	if err != nil {
		t.Fatalf("could not query timescale: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("no rows found in price_history — migration produced no data")
	}

	var createdAt time.Time
	var productID int
	var symbol string
	var markPrice float64
	if err = rows.Scan(&createdAt, &productID, &symbol, &markPrice); err != nil {
		t.Fatalf("could not scan row: %v", err)
	}

	if createdAt.Before(start) {
		t.Errorf("expected created_at >= %v, got %v", start, createdAt)
	}
	if productID != 42 {
		t.Errorf("expected product_id=42, got %d", productID)
	}
	if symbol != "BTC" {
		t.Errorf("expected symbol='BTC', got '%s'", symbol)
	}
	if markPrice != 50000.5 {
		t.Errorf("expected mark_price=50000.5, got %f", markPrice)
	}

	t.Logf("SUCCESS: created_at=%v, product_id=%d, symbol=%s, mark_price=%f",
		createdAt, productID, symbol, markPrice)
}
