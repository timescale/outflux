package influxqueries

import (
	"fmt"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

// MockClient mocks an InfluxDB client
type MockClient struct {
	t                *testing.T
	expectedQuery    string
	expectedResponse *influx.Response
	expectedError    error
	errorInResponse  string
	closeCalled      bool
}

// Ping mock
func (mc *MockClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return timeout, "", nil
}

// Write mock
func (mc *MockClient) Write(bp influx.BatchPoints) error {
	return nil
}

// Query mock
func (mc *MockClient) Query(q influx.Query) (*influx.Response, error) {
	if q.Command != mc.expectedQuery {
		errorString := fmt.Sprintf("Expected <%s> as a query command, got: <%s>", mc.expectedQuery, q.Command)
		mc.t.Error(errorString)
		return nil, fmt.Errorf(errorString)
	}

	return mc.expectedResponse, mc.expectedError
}

// QueryAsChunk mock
func (mc *MockClient) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) {
	return nil, nil
}

// Close mock
func (mc *MockClient) Close() error {
	mc.closeCalled = true
	return nil
}
