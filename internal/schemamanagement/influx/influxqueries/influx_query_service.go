package influxqueries

import (
	"fmt"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
)

// InfluxShowResult contains the results/values from an 'SHOW ' query
type InfluxShowResult struct {
	Values [][]string
}

// InfluxQueryService contains helper functions to work with the InfluxDB client
type InfluxQueryService interface {
	ExecuteQuery(client influx.Client, database, command string) ([]influx.Result, error)
	ExecuteShowQuery(influxClient influx.Client, database, query string) (*InfluxShowResult, error)
}

type defaultInfluxQueryService struct {
}

// NewInfluxQueryService creates a new implementation of the client utils struct
func NewInfluxQueryService() InfluxQueryService {
	return &defaultInfluxQueryService{}
}

// ExecuteQuery sends a command query to an InfluxDB server
func (u *defaultInfluxQueryService) ExecuteQuery(client influx.Client, database, command string) (res []influx.Result, err error) {
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

// ExecuteShowQuery executes a "SHOW ..." InfluxQL query
func (u *defaultInfluxQueryService) ExecuteShowQuery(influxClient influx.Client, database, query string) (*InfluxShowResult, error) {
	if !strings.HasPrefix(strings.ToUpper(query), "SHOW ") {
		return nil, fmt.Errorf("show query must start with 'SHOW '")
	}

	result, err := u.ExecuteQuery(influxClient, database, query)
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
