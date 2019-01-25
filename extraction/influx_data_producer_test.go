package extraction

import (
	"fmt"
	"testing"
	"time"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
)

func TestBuildSelectCommand(t *testing.T) {
	db, measure := "db", "measure"
	oneColumnExample := []*idrf.ColumnInfo{
		&idrf.ColumnInfo{Name: "col1", DataType: idrf.IDRFBoolean, ForeignKey: nil},
	}

	twoColumnExample := []*idrf.ColumnInfo{
		&idrf.ColumnInfo{Name: "col1", DataType: idrf.IDRFBoolean, ForeignKey: nil},
		&idrf.ColumnInfo{Name: "col 2", DataType: idrf.IDRFBoolean, ForeignKey: nil},
	}

	testCases := []struct {
		config   *config.MeasureExtraction
		columns  []*idrf.ColumnInfo
		expected string
	}{
		{ // one column, no time boundaries
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\" FROM \"measure\"",
		}, { // two columns, space in one columns name, lower boundry
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "", ChunkSize: 1},
			columns:  twoColumnExample,
			expected: "SELECT \"col1\", \"col 2\" FROM \"measure\" WHERE time >= 'from'",
		}, { // one column, upper boundry
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "to", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\" FROM \"measure\" WHERE time <= 'to'",
		}, { //double bounded
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "to", ChunkSize: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\" FROM \"measure\" WHERE time >= 'from' AND time <= 'to'",
		}, { //double bounded with limit
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "from", To: "to", ChunkSize: 1, Limit: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\" FROM \"measure\" WHERE time >= 'from' AND time <= 'to' LIMIT 1",
		}, { //no boundes with limit
			config:   &config.MeasureExtraction{Database: db, Measure: measure, From: "", To: "", ChunkSize: 1, Limit: 1},
			columns:  oneColumnExample,
			expected: "SELECT \"col1\" FROM \"measure\" LIMIT 1",
		},
	}

	for _, testCase := range testCases {
		result := buildSelectCommand(testCase.config, testCase.columns)

		if result != testCase.expected {
			t.Errorf("expected query: %s\n, got query: %s", testCase.expected, result)
		}
	}
}

func TestFetchWhenErrorsHappenOnClientCreate(t *testing.T) {
	mockUtils := clientutils.NewUtilsWith(errorReturningGenerator(), nil)
	producer := defaultDataProducer{mockUtils}
	query := influx.Query{}
	dataChannel := make(chan idrf.Row)
	// since we're not starting the Fetch method in a goroutine it will block
	// if nobody is listening to the channel, hence we make it a buffered one
	errorChannel := make(chan error, 1)
	producer.Fetch(nil, dataChannel, errorChannel, query)

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
	mockUtils := clientutils.NewUtilsWith(mockReturningGenerator(mockedClient), nil)

	producer := defaultDataProducer{mockUtils}

	query := influx.Query{}
	dataChannel := make(chan idrf.Row)
	// since we're not starting the Fetch method in a goroutine it will block
	// if nobody is listening to the channel, hence we make it a buffered one
	errorChannel := make(chan error, 1)
	producer.Fetch(nil, dataChannel, errorChannel, query)

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

type mockClientGeneratorDP struct {
	resToReturn influx.Client
	errToReturn error
}

func (cg *mockClientGeneratorDP) CreateInfluxClient(params *clientutils.ConnectionParams) (influx.Client, error) {
	return cg.resToReturn, cg.errToReturn
}

func errorReturningGenerator() *mockClientGeneratorDP {
	return &mockClientGeneratorDP{nil, fmt.Errorf("some error")}
}

func mockReturningGenerator(mockClient influx.Client) *mockClientGeneratorDP {
	return &mockClientGeneratorDP{mockClient, nil}
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
