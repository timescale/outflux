package main

import (
	"sync"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type mockService struct {
	pipes         []pipeline.Pipe
	inflConn      influx.Client
	inflConnErr   error
	inflSchemMngr schemamanagement.SchemaManager
}

func (m *mockService) Create(con *pipeline.ConnectionConfig, arg *pipeline.MigrationConfig) []pipeline.Pipe {
	return m.pipes
}

func (m *mockService) NewConnection(arg *connections.InfluxConnectionParams) (influx.Client, error) {
	return m.inflConn, m.inflConnErr
}

func (m *mockService) Influx(c influx.Client, a string) schemamanagement.SchemaManager {
	return m.inflSchemMngr
}

func (m *mockService) TimeScale(dbConn *pgx.Conn) schemamanagement.SchemaManager { return nil }

type runCounter struct {
	lock        sync.Mutex
	maxRunning  int32
	currRunning int32
}
type mockPipe struct {
	counter *runCounter
	runErr  error
}

func (m *mockPipe) ID() string { return "id" }
func (m *mockPipe) Run() error {
	if m.counter != nil {
		m.counter.lock.Lock()
		m.counter.currRunning++
		if m.counter.currRunning > m.counter.maxRunning {
			m.counter.maxRunning = m.counter.currRunning
		}
		m.counter.lock.Unlock()
		m.counter.lock.Lock()
		m.counter.currRunning--
		m.counter.lock.Unlock()
	}
	return m.runErr
}
