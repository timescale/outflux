package extraction

import (
	"fmt"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

func TestBuildSelectCommand(t *testing.T) {
	db, measure := "db", "measure"
	oneColumnExample := []*idrf.ColumnInfo{
		{Name: "col1", DataType: idrf.IDRFBoolean, ForeignKey: nil},
	}

	twoColumnExample := []*idrf.ColumnInfo{
		{Name: "col1", DataType: idrf.IDRFBoolean, ForeignKey: nil},
		{Name: "col 2", DataType: idrf.IDRFBoolean, ForeignKey: nil},
	}

	testCases := []struct {
		config   *config.MeasureExtraction
		columns  []*idrf.ColumnInfo
		expected string
	}{
		{ // one column, no time boundaries
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\"\nFROM \"measure\"",
		}, { // two columns, space in one columns name, lower boundry
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "", ChunkSize: 1},
			columns:  twoColumnExample,
			expected: "SELECT \"col1\", \"col 2\"\nFROM \"measure\"\nWHERE time >= 'from'",
		}, { // one column, upper boundry
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "to", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\"\nFROM \"measure\"\nWHERE time <= 'to'",
		}, { //double bounded
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "to", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\"\nFROM \"measure\"\nWHERE time >= 'from' AND time <= 'to'",
		}, { //double bounded with limit
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "to", ChunkSize: 1, Limit: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\"\nFROM \"measure\"\nWHERE time >= 'from' AND time <= 'to' \nLIMIT 1",
		}, { //no boundes with limit
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "", ChunkSize: 1, Limit: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\"\nFROM \"measure\" \nLIMIT 1",
		},
	}

	for _, testCase := range testCases {
		result := buildSelectCommand(testCase.config, testCase.columns)

		if result != testCase.expected {
			t.Errorf("expected query: %s, got query: %s", testCase.expected, result)
		}
	}
}

func TestFetchWhenErrorsHappenOnErrSub(t *testing.T) {
	producer := defaultDataProducer{"id", &mockConnService{}, nil}
	query := influx.Query{}
	dataChannel := make(chan idrf.Row)
	errorChannel := make(chan error, 1)

	errorBroadcaster := &mockErrBc{broadChan: errorChannel, subErr: fmt.Errorf("error")}
	producer.Fetch(nil, dataChannel, query, errorBroadcaster)

	producedError := <-errorChannel
	if producedError == nil {
		t.Error("expected an error to be produced by the client generator")
	}

	for range dataChannel {
		t.Error("no data was expected to be received on the data channel")
	}
}
func TestFetchWhenErrorsHappenOnClientCreate(t *testing.T) {
	producer := defaultDataProducer{"id", errorReturningConnService(), nil}
	query := influx.Query{}
	dataChannel := make(chan idrf.Row)
	errorChannel := make(chan error, 1)
	errorBroadcaster := &mockErrBc{subRes: errorChannel, broadChan: errorChannel}

	// since we're not starting the Fetch method in a goroutine it will block
	// if nobody is listening to the channel, hence we make it a buffered one
	producer.Fetch(nil, dataChannel, query, errorBroadcaster)

	producedError := <-errorChannel
	if producedError == nil {
		t.Error("expected an error to be produced by the client generator")
	}

	for range dataChannel {
		t.Error("no data was expected to be received on the data channel")
	}
}

func TestFetchWhenErrorsHappenOnQueryAsChunk(t *testing.T) {
	mockedClient := errorReturningMockClient()
	producer := defaultDataProducer{"id", mockedConnService(mockedClient), nil}

	query := influx.Query{}
	dataChannel := make(chan idrf.Row)
	errorChannel := make(chan error, 1)
	errorBroadcaster := &mockErrBc{subRes: errorChannel, broadChan: errorChannel}

	producer.Fetch(nil, dataChannel, query, errorBroadcaster)

	producedError := <-errorChannel
	if producedError == nil {
		t.Error("expected an error to be produced by mock client on chunk query")
	}

	for range dataChannel {
		t.Error("no data was expected to be received on the data channel")
	}

	if !mockedClient.closeCalled {
		t.Error("close method not called on influx client")
	}

	if mockedClient.queryAsChunkCalled != 1 {
		t.Errorf("expected QueryAsChunk to be called once, called: %d times", mockedClient.queryAsChunkCalled)
	}
}

type mockConnService struct {
	resToReturn influx.Client
	errToReturn error
}

func (qs *mockConnService) NewConnection(params *connections.InfluxConnectionParams) (influx.Client, error) {
	return qs.resToReturn, qs.errToReturn
}

func errorReturningConnService() *mockConnService {
	return &mockConnService{nil, fmt.Errorf("some error")}
}

func mockedConnService(mockClient influx.Client) *mockConnService {
	return &mockConnService{mockClient, nil}
}

type mockClientDP struct {
	closeCalled        bool
	chunkResToReturn   *influx.ChunkedResponse
	chunkErrToReturn   error
	queryAsChunkCalled int
}

func (mc *mockClientDP) Ping(timeout time.Duration) (time.Duration, string, error) {
	return timeout, "", nil
}

func (mc *mockClientDP) Write(bp influx.BatchPoints) error { return nil }

// Query mock
func (mc *mockClientDP) Query(q influx.Query) (*influx.Response, error) { return nil, nil }

// QueryAsChunk mock
func (mc *mockClientDP) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) {
	mc.queryAsChunkCalled++
	return mc.chunkResToReturn, mc.chunkErrToReturn
}

// Close mock
func (mc *mockClientDP) Close() error {
	mc.closeCalled = true
	return nil
}

func errorReturningMockClient() *mockClientDP {
	return &mockClientDP{chunkErrToReturn: fmt.Errorf("some error")}
}

func chunkReturningMockClient(resToReturn *influx.ChunkedResponse) *mockClientDP {
	return &mockClientDP{chunkErrToReturn: nil,
		chunkResToReturn: resToReturn}
}

type mockErrBc struct {
	subRes    chan error
	broadChan chan error
	subErr    error
	unsubErr  error
}

func (e *mockErrBc) Subscribe(id string) (chan error, error) {
	return e.subRes, e.subErr
}

func (e *mockErrBc) Unsubscribe(id string) error {
	return e.unsubErr
}

func (e *mockErrBc) Broadcast(source string, err error) {
	e.broadChan <- err
}

func (e *mockErrBc) Close() {}
