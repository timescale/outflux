package influxschemadiscovery

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	influxModels "github.com/influxdata/influxdb/models"
)

func TestCreateInfluxClient(t *testing.T) {
	_, err := CreateInfluxClient(nil)
	if err == nil {
		t.Error("Should not be able to create a client without connection params")
	}

	serverParams := ConnectionParams{
		server:   "",
		username: "",
		password: "",
	}

	_, err = CreateInfluxClient(&serverParams)
	if err == nil {
		t.Error("Server address should not be accepted")
	}

	serverParams.server = "http://someaddress"
	influxClient, err := CreateInfluxClient(&serverParams)

	if err != nil || influxClient == nil {
		t.Error("Client should have been created without errors")
	}
}

func TestExecuteInfluxQuery(t *testing.T) {
	cases := []MockClient{
		MockClient{ //Expect client to throw error before getting result
			t:             t,
			expectedQuery: "query 1",
			expectedError: fmt.Errorf("error"),
		}, MockClient{ //Expect client to return a result with an error
			t:             t,
			expectedQuery: "query 2",
			expectedResponse: &influx.Response{
				Err: "some error in response",
			},
			errorInResponse: "some error in response",
		}, MockClient{ // Expect client to return empty result, no error
			t:             t,
			expectedQuery: "query 3",
			expectedResponse: &influx.Response{
				Results: []influx.Result{},
			},
		}, MockClient{ // Expect client to return a non-empty result, no error
			t:             t,
			expectedQuery: "query 4",
			expectedResponse: &influx.Response{
				Results: []influx.Result{
					influx.Result{
						Series: []influxModels.Row{},
					},
				},
			},
		}}

	expectedDatabaseName := "database name"
	for _, mockClient := range cases {
		var client influx.Client
		client = mockClient
		response, err := ExecuteInfluxQuery(&client, expectedDatabaseName, mockClient.expectedQuery)
		if mockClient.expectedError != nil && err != mockClient.expectedError {
			// An error was expected, not from the content of the Response
			t.Errorf("Expected to fail with: <%v>, received error was: <%v>", mockClient.expectedError, err)
		}

		if mockClient.errorInResponse != "" && err.Error() != mockClient.errorInResponse {
			// An error was expected from Response.Error() to be returned
			t.Errorf("Expected to fail with: <%v>, received error was: <%v>", mockClient.errorInResponse, err)
		}

		// No response shold have been returned
		if mockClient.expectedResponse == nil && response != nil {
			t.Errorf("Expected response: nil, receivedResponse: <%v>", response)
		} else if mockClient.expectedResponse != nil && response == nil && mockClient.errorInResponse == "" {
			// It was expected that no response be returned, but not because of an error in the Response content
			t.Errorf("Expected response: <%v>, received: nil", mockClient.expectedResponse)
		} else if response != nil && mockClient.expectedResponse != nil {
			// It was expected that the same object was returned as a response as the expectedResponse
			if response != &mockClient.expectedResponse.Results {
				t.Errorf("Expected response: <%v>, received response: <%v>", mockClient.expectedResponse, response)
			}
		}
	}
}
