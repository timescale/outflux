package influxschemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
)

// CreateInfluxClient creates a new HttpClient to an InfluxDB server
func CreateInfluxClient(connectionParams *ConnectionParams) (*influx.Client, error) {
	if connectionParams == nil {
		return nil, fmt.Errorf("Connection params shouldn't be nil")
	}

	clientConfig := influx.HTTPConfig{
		Addr:     connectionParams.server,
		Username: connectionParams.username,
		Password: connectionParams.password,
	}

	newClient, err := influx.NewHTTPClient(clientConfig)
	return &newClient, err
}

// ExecuteInfluxQuery sends a command query to an InfluxDB server
func ExecuteInfluxQuery(influxClient *influx.Client, databaseName string, command string) (res *[]influx.Result, err error) {
	query := influx.Query{
		Command:  command,
		Database: databaseName,
	}

	if response, err := (*influxClient).Query(query); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}

		res = &response.Results
	} else {
		return res, err
	}

	return res, err
}

// ConnectionParams represents the parameters required to open a InfluxDB connection
type ConnectionParams struct {
	server   string
	username string
	password string
}
