package testutils

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

// PrepareServersForITest Creates a database with the same name on the default influx server and default timescale server
func PrepareServersForITest(db string) error {
	if err := CreateInfluxDB(db); err != nil {
		return err
	}

	return CreateTimescaleDb(db)
}

// ClearServersAfterITest Deletes a database on both the default influx and timescale servers
func ClearServersAfterITest(db string) {
	if err := DeleteInfluxDb(db); err != nil {
		log.Printf("could not delete influx db: %v", err)
	}

	if err := DeleteTimescaleDb(db); err != nil {
		log.Printf("could not delete influx db: %v", err)
	}
}

func newInfluxClient() (influx.Client, error) {
	clientConfig := influx.HTTPConfig{
		Addr: InfluxHost,
	}

	return influx.NewHTTPClient(clientConfig)
}

// CreateInfluxDB creates a new influx database to the default influx server. Used for integration tests
func CreateInfluxDB(db string) error {
	queryService := influxqueries.NewInfluxQueryService()
	newClient, err := newInfluxClient()
	if err != nil {
		return err
	}
	_, err = queryService.ExecuteQuery(newClient, db, "CREATE DATABASE "+db)
	newClient.Close()
	return err
}

// DeleteInfluxDb deletes a influx database on the default influx server. Used for integration tests
func DeleteInfluxDb(db string) error {
	queryService := influxqueries.NewInfluxQueryService()
	client, err := newInfluxClient()
	if err != nil {
		return err
	}

	_, err = queryService.ExecuteQuery(client, db, "DROP DATABASE "+db)
	client.Close()
	return err

}

// CreateInfluxMeasure creates a measure with the specified name. For each point the tags and field values are given
// as maps
func CreateInfluxMeasure(db, measure string, tags []*map[string]string, values []*map[string]interface{}) error {
	client, err := newInfluxClient()
	if err != nil {
		return err
	}

	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: db})

	for i, tagSet := range tags {
		point, _ := influx.NewPoint(
			measure,
			*tagSet,
			*values[i],
		)
		bp.AddPoint(point)
	}

	client.Close()
	return client.Write(bp)
}

// CreateInfluxMeasureWithRP creates a measure with the specified name and specified RP. For each point the tags and field values are given
// as maps
func CreateInfluxMeasureWithRP(db, rp, measure string, tags []*map[string]string, values []*map[string]interface{}) error {
	client, err := newInfluxClient()
	if err != nil {
		return err
	}

	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: db, RetentionPolicy: rp})

	for i, tagSet := range tags {
		point, _ := influx.NewPoint(
			measure,
			*tagSet,
			*values[i],
		)
		bp.AddPoint(point)
	}

	client.Close()
	return client.Write(bp)
}

// CreateInfluxRP creates a retention policy with the specified name and 1 day duration
// as maps
func CreateInfluxRP(db, rp string) error {
	client, err := newInfluxClient()
	if err != nil {
		return err
	}

	queryStr := fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON %s DURATION 1d REPLICATION 1`, rp, db)
	query := influx.NewQuery(queryStr, db, "")

	_, err = client.Query(query)
	client.Close()
	return err
}

// CreateTimescaleDb creates a new database on the default server and then creates the extension on it
func CreateTimescaleDb(db string) error {
	dbConn, err := OpenTSConn(defaultPgDb)
	if err != nil {
		return err
	}
	_, err = dbConn.Exec("CREATE DATABASE " + db)
	dbConn.Close()
	return err
}

// CreateTimescaleSchema creates a new schema in the specified db
func CreateTimescaleSchema(db, schema string) error {
	dbConn, err := OpenTSConn(db)
	if err != nil {
		return err
	}
	_, err = dbConn.Exec("CREATE SCHEMA " + schema)
	dbConn.Close()
	return err
}

// OpenTSConn opens a connection to a TimescaleDB
func OpenTSConn(db string) (*pgx.Conn, error) {
	connString := fmt.Sprintf(TsConnStringTemplate, db)
	connConfig, _ := pgx.ParseConnectionString(connString)
	log.Printf("opening ts conn to '%s' with: %s", db, connString)
	return pgx.Connect(connConfig)
}

// DeleteTimescaleDb drops a databass on the default server
func DeleteTimescaleDb(db string) error {
	dbConn, err := OpenTSConn(defaultPgDb)
	if err != nil {
		return err
	}

	_, err = dbConn.Exec("DROP DATABASE IF EXISTS " + db)
	dbConn.Close()
	return err
}
