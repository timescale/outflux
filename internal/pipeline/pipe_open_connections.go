package pipeline

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/connections"
)

func (p *defPipe) openConnections() (influx.Client, *pgx.Conn, error) {
	influxConn, err := p.infConnService.NewConnection(influxConnParams(p.conf.connections))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: could not open connection to Influx Server\n%v", err)
	}

	tsConn, err := p.tsConnService.NewConnection(p.conf.connections.OutputDbConnString)
	if err != nil {
		influxConn.Close()
		return nil, nil, fmt.Errorf("%s: could not open connection to TimescaleDB Server\n%v", err)
	}

	return influxConn, tsConn, nil
}

func influxConnParams(connParams *ConnectionConfig) *connections.InfluxConnectionParams {
	return &connections.InfluxConnectionParams{
		Server:   connParams.InputHost,
		Database: connParams.InputDb,
		Username: connParams.InputUser,
		Password: connParams.InputPass,
	}
}
