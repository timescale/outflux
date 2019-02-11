package influxqueries

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	influxModels "github.com/influxdata/influxdb/models"
)

func TestExecuteQuery(t *testing.T) {
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
	queryExecutor := &defaultInfluxQueryService{}
	for _, mockClient := range cases {
		var client influx.Client
		client = &mockClient
		response, err := queryExecutor.ExecuteQuery(client, expectedDatabaseName, mockClient.expectedQuery)
		if mockClient.expectedError != nil && err != mockClient.expectedError {
			// An error was expected, not from the content of the Response
			t.Errorf("Expected to fail with: <%v>, received error was: <%v>", mockClient.expectedError, err)
		}

		if mockClient.errorInResponse != "" && err.Error() != mockClient.errorInResponse {
			// An error was expected from Response.Error() to be returned
			t.Errorf("expected to fail with: %v, received error was: %v", mockClient.errorInResponse, err)
		}

		// No response shold have been returned
		if mockClient.expectedResponse == nil && response != nil {
			t.Errorf("expected response: nil, receivedResponse: %v", response)
		} else if mockClient.expectedResponse != nil && response == nil && mockClient.errorInResponse == "" {
			// It was expected that no response be returned, but not because of an error in the Response content
			t.Errorf("expected response: %v, received: nil", mockClient.expectedResponse)
		} else if response != nil && mockClient.expectedResponse != nil {
			// It was expected that the same object was returned as a response as the expectedResponse
			if len(response) != len(mockClient.expectedResponse.Results) {
				t.Errorf(
					"expected response length: %d, received response length: %d",
					len(mockClient.expectedResponse.Results),
					len(response),
				)
			}
		}
	}
}

func TestExecuteShowQueryWithFailure(t *testing.T) {
	database := "database"
	queryExecutor := &defaultInfluxQueryService{}
	_, err := queryExecutor.ExecuteShowQuery(nil, database, "NO SHOW query")
	if err == nil {
		t.Error("expected to fail because query didn't start with 'SHOW '")
	}

	badCases := []MockClient{
		MockClient{ //Expect error to be thrown when executing the query, no response
			t:             t,
			expectedQuery: "ShOw something0",
			expectedError: fmt.Errorf("error"),
		}, MockClient{ //Expect client to return a single result with no errors
			t:             t,
			expectedQuery: "SHOW something1",
			expectedResponse: &influx.Response{
				Results: []influx.Result{
					influx.Result{},
					influx.Result{},
				},
			},
		}, MockClient{ // Expect client to return a single result with multiple series
			t:             t,
			expectedQuery: "SHOW something2",
			expectedResponse: &influx.Response{
				Results: []influx.Result{
					influx.Result{
						Series: []influxModels.Row{
							influxModels.Row{},
							influxModels.Row{},
						},
					},
				},
			},
		}, MockClient{ // Expect client to return a result with values not castable to string
			t:             t,
			expectedQuery: "SHOW something3",
			expectedResponse: &influx.Response{
				Results: []influx.Result{
					influx.Result{
						Series: []influxModels.Row{
							influxModels.Row{
								Values: [][]interface{}{[]interface{}{1}},
							},
						},
					},
				},
			},
		}}

	for _, badCase := range badCases {
		var client influx.Client
		client = &badCase
		_, err := queryExecutor.ExecuteShowQuery(client, database, badCase.expectedQuery)
		if err == nil {
			t.Error("error not returned when expecting ")
		}
	}

}

func TestExecuteShowQueryWithOkResults(t *testing.T) {
	database := "database"
	goodQuery := "SHOW something"
	goodValue := "1"
	var goodCaseWithResults influx.Client
	goodCaseWithResults = &MockClient{
		t:             t,
		expectedQuery: goodQuery,
		expectedResponse: &influx.Response{
			Results: []influx.Result{
				influx.Result{
					Series: []influxModels.Row{
						influxModels.Row{
							Values: [][]interface{}{[]interface{}{goodValue}},
						},
					},
				},
			},
		},
	}

	queryExecutor := &defaultInfluxQueryService{}

	response, err := queryExecutor.ExecuteShowQuery(goodCaseWithResults, database, goodQuery)
	if err != nil {
		t.Errorf("Expected no error to happen. Got '%s'", err.Error())
	}

	if response == nil || response.Values == nil {
		t.Errorf("Expected a response with non-nil values. Got %v", response)
	}

	values := response.Values
	if len(values) != 1 || len(values[0]) != 1 && values[0][0] != goodValue {
		t.Errorf("Expected one row with one value and that value to be '%s', but got '%v'", goodValue, response)
	}

	var goodCaseNoResults influx.Client
	goodCaseNoResults = &MockClient{
		t:             t,
		expectedQuery: goodQuery,
		expectedResponse: &influx.Response{
			Results: []influx.Result{
				influx.Result{
					Series: []influxModels.Row{},
				},
			},
		},
	}

	response, err = queryExecutor.ExecuteShowQuery(goodCaseNoResults, database, goodQuery)
	if err != nil {
		t.Errorf("Expected no error to happen. Got '%s'", err.Error())
	}

	if response == nil || response.Values == nil {
		t.Errorf("Expected a response with non-nil values. Got %v", response)
	}

	values = response.Values
	if len(values) != 0 {
		t.Errorf("Expected an empty values matrix, but got '%v'", response)
	}
}
