package testutils

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

// PrepareServersForITest Creates a database with the same name on the default influx server and default timescale server
func PrepareServersForITest(t *testing.T, db string) {
	CreateInfluxDB(t, db)
	CreateTimescaleDb(t, db)
}

// ClearServersAfterITest Deletes a database on both the default influx and timescale servers
func ClearServersAfterITest(t *testing.T, db string) {
	DeleteInfluxDb(t, db)
	DeleteTimescaleDb(t, db)
}

func newInfluxClient() (influx.Client, error) {
	clientConfig := influx.HTTPConfig{
		Addr: InfluxHost,
	}

	return influx.NewHTTPClient(clientConfig)
}

// CreateInfluxDB creates a new influx database to the default influx server. Used for integration tests
func CreateInfluxDB(t *testing.T, db string) {
	queryService := influxqueries.NewInfluxQueryService()
	newClient, err := newInfluxClient()
	panicOnErr(t, err)
	_, err = queryService.ExecuteQuery(newClient, db, "CREATE DATABASE "+db)
	panicOnErr(t, err)
}

// DeleteInfluxDb deletes a influx database on the default influx server. Used for integration tests
func DeleteInfluxDb(t *testing.T, db string) {
	queryService := influxqueries.NewInfluxQueryService()
	client, err := newInfluxClient()
	panicOnErr(t, err)
	_, err = queryService.ExecuteQuery(client, db, "DROP DATABASE "+db)
	panicOnErr(t, err)
}

// CreateInfluxMeasure creates a measure with the specified name. For each point the tags and field values are given
// as maps
func CreateInfluxMeasure(t *testing.T, db, measure string, tags []*map[string]string, values []*map[string]interface{}) {
	client, err := newInfluxClient()
	panicOnErr(t, err)

	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: db})

	for i, tagSet := range tags {
		point, _ := influx.NewPoint(
			measure,
			*tagSet,
			*values[i],
		)
		bp.AddPoint(point)

	}

	err = client.Write(bp)
	panicOnErr(t, err)
	client.Close()
}

// CreateTimescaleDb creates a new databas on the default server and then creates the extension on it
func CreateTimescaleDb(t *testing.T, db string) {
	dbConn := OpenTSConn(defaultPgDb)
	defer dbConn.Close()
	_, err := dbConn.Exec("CREATE DATABASE " + db)
	panicOnErr(t, err)
}

// OpenTSConn opens a connection to a TimescaleDB
func OpenTSConn(db string) *pgx.Conn {
	connString := fmt.Sprintf(TsConnStringTemplate, db)
	connConfig, _ := pgx.ParseConnectionString(connString)
	c, _ := pgx.Connect(connConfig)
	return c
}

// DeleteTimescaleDb drops a databass on the default server
func DeleteTimescaleDb(t *testing.T, db string) {
	dbConn := OpenTSConn(defaultPgDb)
	defer dbConn.Close()
	_, err := dbConn.Exec("DROP DATABASE " + db)
	panicOnErr(t, err)
}

func panicOnErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
