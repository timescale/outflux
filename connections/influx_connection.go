package connections

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
)

// InfluxConnectionParams represents the parameters required to open a InfluxDB connection
type InfluxConnectionParams struct {
	Server   string
	Username string
	Password string
	Database string
}

// InfluxConnectionService creates new clients connected to some Influx server
type InfluxConnectionService interface {
	NewConnection(*InfluxConnectionParams) (influx.Client, error)
}

type defaultInfluxConnectionService struct{}

// NewInfluxConnectionService creates a new instance of the service
func NewInfluxConnectionService() InfluxConnectionService {
	return &defaultInfluxConnectionService{}
}

func (s *defaultInfluxConnectionService) NewConnection(params *InfluxConnectionParams) (influx.Client, error) {
	if params == nil {
		return nil, fmt.Errorf("Connection params shouldn't be nil")
	}

	clientConfig := influx.HTTPConfig{
		Addr:     params.Server,
		Username: params.Username,
		Password: params.Password,
	}

	newClient, err := influx.NewHTTPClient(clientConfig)
	return newClient, err
}
