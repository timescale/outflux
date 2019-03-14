package main

import (
	"sync"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type mockService struct {
	pipe          pipeline.Pipe
	pipeErr       error
	inflConn      influx.Client
	inflConnErr   error
	inflSchemMngr schemamanagement.SchemaManager
}

func (m *mockService) Create(influx.Client, *pgx.Conn, string, *cli.ConnectionConfig, *cli.MigrationConfig) (pipeline.Pipe, error) {
	return m.pipe, m.pipeErr
}

func (m *mockService) NewConnection(arg *connections.InfluxConnectionParams) (influx.Client, error) {
	return m.inflConn, m.inflConnErr
}

func (m *mockService) Influx(c influx.Client, a string) schemamanagement.SchemaManager {
	return m.inflSchemMngr
}

func (m *mockService) TimeScale(dbConn *pgx.Conn) schemamanagement.SchemaManager { return nil }

type mockTsConnSer struct {
	tsConn    *pgx.Conn
	tsConnErr error
}

func (m *mockTsConnSer) NewConnection(connStr string) (*pgx.Conn, error) {
	return m.tsConn, m.tsConnErr
}

type runCounter struct {
	lock        *sync.Mutex
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

type mockInfConn struct {
	closeCalled bool
}

func (m *mockInfConn) Ping(timeout time.Duration) (time.Duration, string, error)    { return 0, "", nil }
func (m *mockInfConn) Write(bp influx.BatchPoints) error                            { return nil }
func (m *mockInfConn) Query(q influx.Query) (*influx.Response, error)               { return nil, nil }
func (m *mockInfConn) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) { return nil, nil }
func (m *mockInfConn) Close() error {
	m.closeCalled = true
	return nil
}
