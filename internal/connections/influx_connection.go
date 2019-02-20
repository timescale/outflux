package connections

import (
	"fmt"
	"os"

	influx "github.com/influxdata/influxdb/client/v2"
)

// Environment variable names to be used for the InfluxDB connection
const (
	UserEnvVar = "INFLUX_USERNAME"
	PassEnvVar = "INFLUX_PASSWORD"
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

	var user, pass string

	if params.Username != "" {
		user = params.Username
	} else {
		user = os.Getenv(UserEnvVar)
	}

	if params.Password != "" {
		pass = params.Password
	} else {
		pass = os.Getenv(PassEnvVar)
	}
	clientConfig := influx.HTTPConfig{Addr: params.Server, Username: user, Password: pass}

	newClient, err := influx.NewHTTPClient(clientConfig)
	return newClient, err
}
