package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/cli"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

func TestDiscoverMeasuresErrorNewConnection(t *testing.T) {
	app := &appContext{
		ics: &mockService{inflConnErr: fmt.Errorf("error")},
	}
	connArgs := &cli.ConnectionConfig{}
	res, err := discoverMeasures(app, connArgs)
	if res != nil || err == nil {
		t.Errorf("expected error, none received")
	}
}

func TestDiscoverMeasures(t *testing.T) {
	mockClient := &tdmc{}
	mockSchemaMngr := &tdmsm{}
	mockAll := &mockService{inflConn: mockClient, inflSchemMngr: mockSchemaMngr}
	app := &appContext{
		ics:                  mockAll,
		schemaManagerService: mockAll,
	}
	connArgs := &cli.ConnectionConfig{}
	_, err := discoverMeasures(app, connArgs)
	if err != nil {
		t.Errorf("unexpected error:%v", err)
	}

	if !mockClient.closed || !mockSchemaMngr.discoverCalled {
		t.Errorf("expected closed: true, discover: true\ngot closed:%v, discover:%v", mockClient.closed, mockSchemaMngr.discoverCalled)
	}
}

func TestTransferSchemaErrorOnDiscoverMeasures(t *testing.T) {
	mockAll := &mockService{inflConnErr: fmt.Errorf("error")}
	app := &appContext{ics: mockAll}
	connArgs := &cli.ConnectionConfig{}
	stArgs := &cli.MigrationConfig{}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}
}

func TestSchemaTransferErrorOnOpenConn(t *testing.T) {
	mockClient := &tdmc{}
	mockSchemaMngr := &tdmsm{m: []string{"a"}}
	pipe := &mockPipe{runErr: fmt.Errorf("error"), counter: &runCounter{}}
	mockAll := &mockService{
		inflConn:      mockClient,
		inflSchemMngr: mockSchemaMngr,
		pipe:          pipe,
	}
	mockTsConn := &mockTsConnSer{tsConnErr: fmt.Errorf("error")}
	app := &appContext{ics: mockAll, tscs: mockTsConn, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &cli.ConnectionConfig{}
	stArgs := &cli.MigrationConfig{Quiet: true}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}
}

func TestTransferSchemaErrorOnRun(t *testing.T) {
	mockClient := &tdmc{}
	mockSchemaMngr := &tdmsm{m: []string{"a"}}
	pipe := &mockPipe{runErr: fmt.Errorf("error"), counter: &runCounter{lock: &sync.Mutex{}}}
	mockAll := &mockService{
		inflConn:      mockClient,
		inflSchemMngr: mockSchemaMngr,
		pipe:          pipe,
	}
	mockTsConn := &mockTsConnSer{tsConn: &pgx.Conn{}}
	app := &appContext{ics: mockAll, tscs: mockTsConn, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &cli.ConnectionConfig{}
	stArgs := &cli.MigrationConfig{Quiet: true}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}

	if pipe.counter.maxRunning != 1 {
		t.Errorf("pipe didn't run")
	}
}

func TestErrorOnPipeCreate(t *testing.T) {
	mockClient := &tdmc{}
	mockSchemaMngr := &tdmsm{m: []string{"a"}}
	mockAll := &mockService{
		inflConn:      mockClient,
		inflSchemMngr: mockSchemaMngr,
		pipeErr:       fmt.Errorf("error"),
	}
	mockTsConn := &mockTsConnSer{tsConn: &pgx.Conn{}}
	app := &appContext{ics: mockAll, tscs: mockTsConn, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &cli.ConnectionConfig{}
	stArgs := &cli.MigrationConfig{Quiet: true}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}
}
func TestTransferSchema(t *testing.T) {
	pipe := &mockPipe{counter: &runCounter{lock: &sync.Mutex{}}}
	mockAll := &mockService{
		pipe:     pipe,
		inflConn: &mockInfConn{},
	}
	app := &appContext{ics: mockAll, tscs: &mockTsConnSer{tsConn: &pgx.Conn{}}, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &cli.ConnectionConfig{InputMeasures: []string{"a"}}
	stArgs := &cli.MigrationConfig{}
	err := transferSchema(app, connArgs, stArgs)
	if err != nil {
		t.Errorf("unexpected error:%v", err)
	}

	if pipe.counter.maxRunning != 1 {
		t.Errorf("pipe didn't run")
	}
}

type tdmc struct{ closed bool }

func (t *tdmc) Ping(timeout time.Duration) (time.Duration, string, error)    { return 0, "", nil }
func (t *tdmc) Write(bp influx.BatchPoints) error                            { return nil }
func (t *tdmc) Query(q influx.Query) (*influx.Response, error)               { return nil, nil }
func (t *tdmc) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) { return nil, nil }
func (t *tdmc) Close() error                                                 { t.closed = true; return nil }

type tdmsm struct {
	m              []string
	discoverCalled bool
}

func (t *tdmsm) DiscoverDataSets() ([]string, error)                          { t.discoverCalled = true; return t.m, nil }
func (t *tdmsm) FetchDataSet(dataSetIdentifier string) (*idrf.DataSet, error) { return nil, nil }
func (t *tdmsm) PrepareDataSet(dataSet *idrf.DataSet, strategy schemaconfig.SchemaStrategy) error {
	return nil
}
