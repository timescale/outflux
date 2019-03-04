package main

import (
	"fmt"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/pipeline"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

func TestDiscoverMeasuresErrorNewConnection(t *testing.T) {
	app := &appContext{
		ics: &mockService{inflConnErr: fmt.Errorf("error")},
	}
	connArgs := &pipeline.ConnectionConfig{}
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
	connArgs := &pipeline.ConnectionConfig{}
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
	connArgs := &pipeline.ConnectionConfig{}
	stArgs := &pipeline.MigrationConfig{}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}
}

func TestTransferSchemaErrorOnRun(t *testing.T) {
	mockClient := &tdmc{}
	mockSchemaMngr := &tdmsm{m: []string{"a"}}
	pipe := &mockPipe{runErr: fmt.Errorf("error"), counter: &runCounter{}}
	mockAll := &mockService{
		inflConn:      mockClient,
		inflSchemMngr: mockSchemaMngr,
		pipes:         []pipeline.Pipe{pipe},
	}
	app := &appContext{ics: mockAll, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &pipeline.ConnectionConfig{}
	stArgs := &pipeline.MigrationConfig{Quiet: true}
	err := transferSchema(app, connArgs, stArgs)
	if err == nil {
		t.Errorf("expected err, none got")
	}

	if pipe.counter.maxRunning != 1 {
		t.Errorf("pipe didn't run")
	}
}

func TestTransferSchema(t *testing.T) {
	pipe := &mockPipe{counter: &runCounter{}}
	mockAll := &mockService{
		pipes: []pipeline.Pipe{pipe},
	}
	app := &appContext{ics: mockAll, pipeService: mockAll, schemaManagerService: mockAll}
	connArgs := &pipeline.ConnectionConfig{InputMeasures: []string{"a"}}
	stArgs := &pipeline.MigrationConfig{}
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
