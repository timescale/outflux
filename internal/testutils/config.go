package testutils

const (
	// InfluxHost is the default InfluxDB host to be used in integration tests
	InfluxHost = "http://localhost:8086"
	// TsConnStringTemplate is the conn string for the default Timescale host to be used in integration tests
	TsConnStringTemplate = "user=postgres password=postgres port=5433 dbname=%s sslmode=disable"
	defaultPgDb          = "postgres"
)
