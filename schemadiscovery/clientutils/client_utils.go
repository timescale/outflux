package clientutils

import (
	"fmt"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
)

// InfluxShowResult contains the results/values from an 'SHOW ' query
type InfluxShowResult struct {
	Values [][]string
}

// ConnectionParams represents the parameters required to open a InfluxDB connection
type ConnectionParams struct {
	Server   string
	Username string
	Password string
}

type influxClientGenerator interface {
	CreateInfluxClient(params *ConnectionParams) (influx.Client, error)
}

type influxQueryExecutor interface {
	ExecuteInfluxQuery(client influx.Client, database, command string) ([]influx.Result, error)
}

type showQueryExecutor interface {
	ExecuteShowQuery(influxClient influx.Client, database, query string) (*InfluxShowResult, error)
}

type defaultClientGenerator struct{}

// CreateInfluxClient creates a new HttpClient to an InfluxDB server
func (dcg *defaultClientGenerator) CreateInfluxClient(params *ConnectionParams) (influx.Client, error) {
	if params == nil {
		return nil, fmt.Errorf("Connection params shouldn't be nil")
	}

	clientConfig := influx.HTTPConfig{
		Addr:     params.Server,
		Username: params.Username,
		Password: params.Password,
	}

	newClient, err := influx.NewHTTPClient(clientConfig)
	return newClient, err
}

type defaultQueryExecutor struct{}

// ExecuteInfluxQuery sends a command query to an InfluxDB server
func (dqe *defaultQueryExecutor) ExecuteInfluxQuery(client influx.Client, database, command string) (res []influx.Result, err error) {
	query := influx.Query{
		Command:  command,
		Database: database,
	}

	if response, err := client.Query(query); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}

		res = response.Results
	} else {
		return res, err
	}

	return res, err
}

type defaultShowExecutor struct {
	influxQueryExecutor
}

// ExecuteShowQuery executes a "SHOW ..." InfluxQL query
func (dse *defaultShowExecutor) ExecuteShowQuery(influxClient influx.Client, database, query string) (*InfluxShowResult, error) {
	if !strings.HasPrefix(strings.ToUpper(query), "SHOW ") {
		return nil, fmt.Errorf("show query must start with 'SHOW '")
	}

	result, err := dse.influxQueryExecutor.ExecuteInfluxQuery(influxClient, database, query)
	if err != nil {
		return nil, err
	}

	if len(result) != 1 {
		errorString := "'SHOW' query failed. No results returned."
		return nil, fmt.Errorf(errorString)
	}

	series := result[0].Series
	if len(series) == 0 {
		return &InfluxShowResult{Values: [][]string{}}, nil
	} else if len(series) > 1 {
		errorString := "'SHOW' query returned unexpected results. More than one series found."
		return nil, fmt.Errorf(errorString)
	}

	convertedValues, err := castShowResultValues(series[0].Values)
	if err != nil {
		return nil, err
	}

	return &InfluxShowResult{Values: convertedValues}, nil
}

// ClientUtils contains helper functions to work with the InfluxDB client
type ClientUtils interface {
	influxClientGenerator
	influxQueryExecutor
	showQueryExecutor
}

type defaultUtils struct {
	influxQueryExecutor
	influxClientGenerator
	showQueryExecutor
}

// NewUtils creates a new implementation of the client utils struct
func NewUtils() ClientUtils {
	queryExecutor := &defaultQueryExecutor{}
	return &defaultUtils{
		influxClientGenerator: &defaultClientGenerator{},
		influxQueryExecutor:   queryExecutor,
		showQueryExecutor:     &defaultShowExecutor{queryExecutor},
	}
}

// NewUtilsWith returns a new implementation of ClientUtils with the provided dependencies
func NewUtilsWith(clientGenerator influxClientGenerator, showExecutor showQueryExecutor) ClientUtils {
	return &defaultUtils{
		influxClientGenerator: clientGenerator,
		showQueryExecutor:     showExecutor,
	}
}
func castShowResultValues(returnedResults [][]interface{}) ([][]string, error) {
	toReturn := make([][]string, len(returnedResults))
	var err bool
	for i, row := range returnedResults {
		toReturn[i] = make([]string, len(row))
		for j, value := range row {
			toReturn[i][j], err = value.(string)
			if !err {
				return nil, fmt.Errorf("value from 'SHOW ' query could not be cast to string")
			}
		}
	}

	return toReturn, nil
}
