package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/cli"
)

func TestPreparePipeErrors(t *testing.T) {
	testCases := []struct {
		expected string
		input    []error
	}{
		{
			expected: "Migration finished with errors:\n",
			input:    []error{},
		}, {
			expected: "Migration finished with errors:\nOne\nTwo\n",
			input:    []error{fmt.Errorf("One"), fmt.Errorf("Two")},
		},
	}

	for _, testCase := range testCases {
		res := preparePipeErrors(testCase.input)
		if res.Error() != testCase.expected {
			t.Errorf("expected:%s\ngot:%s", testCase.expected, res.Error())
		}
	}
}
func TestMigrateErrorOnDiscoverMeasures(t *testing.T) {
	app := &appContext{
		pipeService: &mockService{},
		ics:         &mockService{inflConnErr: fmt.Errorf("error")},
	}

	conn := &cli.ConnectionConfig{}
	mig := &cli.MigrationConfig{Quiet: true}
	err := migrate(app, conn, mig)
	if err == nil {
		t.Error("expected error, none received")
	}
}

func TestOpenConnectionsReturnsError(t *testing.T) {
	app := &appContext{
		ics: &mockService{inflConnErr: fmt.Errorf("error")},
	}

	conn := &cli.ConnectionConfig{
		InputMeasures: []string{"a"},
	}
	mig := &cli.MigrationConfig{MaxParallel: 1}
	err := migrate(app, conn, mig)
	if err == nil {
		t.Error("expected error, none received")
	}
}

func TestMigrateCreatePipeReturnsError(t *testing.T) {
	app := &appContext{
		ics:         &mockService{inflConn: &mockInfConn{}},
		tscs:        &mockTsConnSer{tsConn: &pgx.Conn{}},
		pipeService: &mockService{pipeErr: fmt.Errorf("error")},
	}

	conn := &cli.ConnectionConfig{
		InputMeasures: []string{"a"},
	}
	mig := &cli.MigrationConfig{MaxParallel: 1}
	err := migrate(app, conn, mig)
	if err == nil {
		t.Error("expected error, none received")
	}
}
func TestMigratePipeReturnsError(t *testing.T) {
	errorReturningPipe := &mockPipe{runErr: fmt.Errorf("error")}
	app := &appContext{
		ics:  &mockService{inflConn: &mockInfConn{}},
		tscs: &mockTsConnSer{tsConn: &pgx.Conn{}},
		pipeService: &mockService{
			pipe: errorReturningPipe,
		},
	}
	conn := &cli.ConnectionConfig{InputMeasures: []string{"a"}}
	mig := &cli.MigrationConfig{MaxParallel: 1}
	err := migrate(app, conn, mig)
	if err == nil {
		t.Errorf("expected error, none received")
	}
}

func TestMigratePipesWaitForSemaphore(t *testing.T) {
	counter := &runCounter{lock: &sync.Mutex{}}
	goodPipe1 := &mockPipe{counter: counter}

	app := &appContext{
		pipeService: &mockService{
			pipe: goodPipe1,
		},
		ics:  &mockService{inflConn: &lockedConnMock{lock: &sync.Mutex{}}},
		tscs: &mockTsConnSer{tsConn: &pgx.Conn{}},
	}
	conn := &cli.ConnectionConfig{InputMeasures: []string{"a", "b", "c"}}
	mig := &cli.MigrationConfig{MaxParallel: 2}
	err := migrate(app, conn, mig)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if counter.maxRunning > int32(mig.MaxParallel) {
		t.Errorf("number of concurrent pipelines (%d) was too damn high (allowed %d)", counter.maxRunning, mig.MaxParallel)
	}

	if !app.ics.(*mockService).inflConn.(*lockedConnMock).closeCalled {
		t.Errorf("close not called on influx client")
	}
}

type lockedConnMock struct {
	lock        *sync.Mutex
	closeCalled bool
}

func (m *lockedConnMock) Ping(timeout time.Duration) (time.Duration, string, error) { return 0, "", nil }
func (m *lockedConnMock) Write(bp influx.BatchPoints) error                         { return nil }
func (m *lockedConnMock) Query(q influx.Query) (*influx.Response, error)            { return nil, nil }
func (m *lockedConnMock) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) {
	return nil, nil
}
func (m *lockedConnMock) Close() error {
	m.lock.Lock()
	m.closeCalled = true
	m.lock.Unlock()
	return nil
}
func TestOpenConnections(t *testing.T) {
	// error on new influx con
	app := &appContext{
		ics: &mockService{
			inflConnErr: fmt.Errorf("some error"),
		},
	}

	// error on open influx conn
	_, _, err := openConnections(app, &cli.ConnectionConfig{})
	if err == nil {
		t.Errorf("expected error, none received")
	}

	// error on open ts conn
	mockIcs := &mockService{inflConn: &mockInfConn{}}
	mockTs := &mockTsConnSer{tsConnErr: fmt.Errorf("error")}
	app = &appContext{
		ics:  mockIcs,
		tscs: mockTs,
	}
	_, _, err = openConnections(app, &cli.ConnectionConfig{})
	if err == nil {
		t.Error("expected error, none received")
	} else if !mockIcs.inflConn.(*mockInfConn).closeCalled {
		t.Error("close not called on influx connection")
	}

	// no error
	mockIcs = &mockService{inflConn: &mockInfConn{}}
	mockTs = &mockTsConnSer{tsConn: &pgx.Conn{}}
	app = &appContext{
		ics:  mockIcs,
		tscs: mockTs,
	}
	_, _, err = openConnections(app, &cli.ConnectionConfig{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if mockIcs.inflConn.(*mockInfConn).closeCalled {
		t.Error("close method was called on influx connection")
	}
}
