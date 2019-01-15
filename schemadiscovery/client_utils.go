package schemadiscovery

import (
	"fmt"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
)

type InlfuxShowResult struct {
	values [][]string
}

// CreateInfluxClient creates a new HttpClient to an InfluxDB server
func CreateInfluxClient(connectionParams *ConnectionParams) (*influx.Client, error) {
	if connectionParams == nil {
		return nil, fmt.Errorf("Connection params shouldn't be nil")
	}

	clientConfig := influx.HTTPConfig{
		Addr:     connectionParams.server,
		Username: connectionParams.username,
		Password: connectionParams.password,
	}

	newClient, err := influx.NewHTTPClient(clientConfig)
	return &newClient, err
}

// ExecuteInfluxQuery sends a command query to an InfluxDB server
func ExecuteInfluxQuery(influxClient *influx.Client, databaseName, command string) (res *[]influx.Result, err error) {
	query := influx.Query{
		Command:  command,
		Database: databaseName,
	}

	if response, err := (*influxClient).Query(query); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}

		res = &response.Results
	} else {
		return res, err
	}

	return res, err
}

// ConnectionParams represents the parameters required to open a InfluxDB connection
type ConnectionParams struct {
	server   string
	username string
	password string
}

// ExecuteShowQuery executes a "SHOW ..." InfluxQL query
func ExecuteShowQuery(influxClient *influx.Client, database, query string) (*InlfuxShowResult, error) {
	if !strings.HasPrefix(query, "SHOW ") {
		return nil, fmt.Errorf("show query must start with 'SHOW '")
	}

	resultPtr, err := ExecuteInfluxQuery(influxClient, database, query)
	if err != nil {
		return nil, err
	}

	result := *resultPtr
	if len(result) != 1 {
		errorString := "'SHOW' query failed. No results returned."
		return nil, fmt.Errorf(errorString)
	}

	series := result[0].Series
	if len(series) == 0 {
		return &InfluxShowResult{values: [][]string{}}, nil
	} else if len(series) > 1 {
		errorString := "'SHOW' query returned unexpected results. More than one series found."
		return nil, fmt.Errorf(errorString)
	}

	convertedValues := castShowResultValues(values[0].Values)
	return &InfluxShowResult{values: convertedValues}, nil
}

func castShowResultValues(returnedResults [][]interface{}) ([][]string, error) {
	toReturn := make([][]string, len(returnedResults))
	for i, row := range returnedResults {
		toReturn[i] = make([]string, len(row))
		for j, value := range row {
			toReturn[i][j], err = value.(string)
			if err != nil {
				return nil, fmt.Println("value from 'SHOW ' query could not be cast to string")
			}
		}
	}

	return toReturn, nil
}
