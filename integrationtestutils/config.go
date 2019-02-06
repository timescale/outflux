package integrationtestutils

const (
	// InfluxHost is the default InfluxDB host to be used in integration tests
	InfluxHost = "http://localhost:8086"
	// TsHost is the default Timescale host to be used in integration tests
	TsHost      = "localhost:5433"
	defaultPgDb = "postgres"
	// TsUser is the default user that connects to Timescale in integration tests
	TsUser = "postgres"
	// TsPass is the default password used toconnect to Timescale in integration tests
	TsPass = "postgres"
)
