package integrationtestutils

import (
	"database/sql"
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// PrepareServersForITest Creates a database with the same name on the default influx server and default timescale server
func PrepareServersForITest(db string) {
	CreateInfluxDb(db)
	CreateTimescaleDb(db)
}

// ClearServersAfterITest Deletes a database on both the default influx and timescale servers
func ClearServersAfterITest(db string) {
	DeleteInfluxDb(db)
	DeleteTimescaleDb(db)
}

// CreateInfluxDb creates a new influx database to the default influx server. Used for integration tests
func CreateInfluxDb(db string) {
	clientUtils := clientutils.NewUtils()
	client, err := clientUtils.CreateInfluxClient(&clientutils.ConnectionParams{
		Server:   InfluxHost,
		Username: "",
		Password: ""})
	maybePanic(err)
	_, err = clientUtils.ExecuteInfluxQuery(client, db, "CREATE DATABASE "+db)
	maybePanic(err)
}

// DeleteInfluxDb deletes a influx database on the default influx server. Used for integration tests
func DeleteInfluxDb(db string) {
	clientUtils := clientutils.NewUtils()
	client, err := clientUtils.CreateInfluxClient(&clientutils.ConnectionParams{
		Server:   InfluxHost,
		Username: "",
		Password: ""})
	maybePanic(err)
	_, err = clientUtils.ExecuteInfluxQuery(client, db, "DROP DATABASE "+db)
	maybePanic(err)
}

// CreateInfluxMeasure creates a measure with the specified name. For each point the tags and field values are given
// as maps
func CreateInfluxMeasure(db, measure string, tags []*map[string]string, values []*map[string]interface{}) {
	clientUtils := clientutils.NewUtils()
	client, err := clientUtils.CreateInfluxClient(&clientutils.ConnectionParams{
		Server:   InfluxHost,
		Username: "",
		Password: ""})
	maybePanic(err)

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
	maybePanic(err)
	client.Close()
}

// CreateTimescaleDb creates a new databas on the default server and then creates the extension on it
func CreateTimescaleDb(db string) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", TsUser, TsPass, TsHost, defaultPgDb)
	dbConn, err := sql.Open("postgres", connStr)
	maybePanic(err)
	_, err = dbConn.Query("CREATE DATABASE " + db)
	maybePanic(err)
	dbConn.Close()
}

func OpenTsConn(db string) *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", TsUser, TsPass, TsHost, db)
	dbConn, err := sql.Open("postgres", connStr)
	maybePanic(err)
	return dbConn
}

// ExecuteTsQuery executes a supplied query to the default server
func ExecuteTsQuery(db, query string) *sql.Rows {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", TsUser, TsPass, TsHost, db)
	dbConn, err := sql.Open("postgres", connStr)
	maybePanic(err)
	rows, err := dbConn.Query(query)
	maybePanic(err)
	dbConn.Close()
	return rows
}

// DeleteTimescaleDb drops a databass on the default server
func DeleteTimescaleDb(db string) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", TsUser, TsPass, TsHost, defaultPgDb)
	dbConn, err := sql.Open("postgres", connStr)
	maybePanic(err)
	_, err = dbConn.Query("DROP DATABASE " + db)
	maybePanic(err)
	dbConn.Close()
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}
