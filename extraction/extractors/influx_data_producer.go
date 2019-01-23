package extractors

import (
	"fmt"
	"io"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

// DataProducer populates a data channel with the results from an influx query
type DataProducer interface {
	Fetch(connectionParams *clientutils.ConnectionParams,
		dataChannel chan idrf.Row,
		errorChannel chan error,
		query influx.Query)
}

type defaultDataProducer struct {
	influxUtils clientutils.ClientUtils
}

// NewDataProducer creates a new implementation of the data producer
func NewDataProducer() DataProducer {
	return &defaultDataProducer{
		influxUtils: clientutils.NewUtils(),
	}
}

// NewDataProducerWith creates a new implementation of the data producer with a supplied client utils
func NewDataProducerWith(influxUtils clientutils.ClientUtils) DataProducer {
	return &defaultDataProducer{
		influxUtils: influxUtils,
	}
}

// Executes the select query and receives the chunked response, piping it to a data channel.
// If an error occurs a single error is sent to the error channel. Both channels are closed at the end of the routine.
func (dp *defaultDataProducer) Fetch(connectionParams *clientutils.ConnectionParams,
	dataChannel chan idrf.Row,
	errorChannel chan error,
	query influx.Query) {
	defer close(dataChannel)
	defer close(errorChannel)

	client, err := dp.influxUtils.CreateInfluxClient(connectionParams)

	if err != nil {
		errorChannel <- err
		return
	}

	defer client.Close()

	chunkResponse, err := client.QueryAsChunk(query)
	if err != nil {
		errorChannel <- err
		return
	}

	defer chunkResponse.Close()

	for {
		response, err := chunkResponse.NextResponse()
		if err != nil {
			if err == io.EOF {
				return
			}

			// If we got an error while decoding the response, send that back.
			errorChannel <- err
			return
		}

		if response == nil {
			errorChannel <- fmt.Errorf("server did not return a proper response")
			return
		}

		if response.Err != "" {
			errorChannel <- fmt.Errorf("returned response had an error: %s", response.Err)
			return
		}

		if len(response.Results) != 1 {
			errorChannel <- fmt.Errorf("returned response had an unexpected format")
			return
		}

		series := response.Results[0].Series
		if len(series) > 1 {
			errorChannel <- fmt.Errorf("returned response had an unexpected format")
			return
		} else if len(series) == 0 {
			return
		}

		rows := series[0]
		for _, valRow := range rows.Values {
			dataChannel <- valRow
		}
	}
}

func buildSelectCommand(config *config.MeasureExtraction, columns []*idrf.ColumnInfo) string {
	projection := buildProjection(columns)
	if config.From != "" && config.To != "" {
		return fmt.Sprintf(selectQueryDoubleBoundTemplate, projection, config.Measure, config.From, config.To)
	} else if config.From != "" {
		return fmt.Sprintf(selectQueryLowerBoundTemplate, projection, config.Measure, config.From)
	} else if config.To != "" {
		return fmt.Sprintf(selectQueryUpperBoundTemplate, projection, config.Measure, config.To)
	} else {
		return fmt.Sprintf(selectQueryNoBoundTemplate, projection, config.Measure)
	}
}

func buildProjection(columns []*idrf.ColumnInfo) string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = fmt.Sprintf("\"%s\"", column.Name)
	}

	return strings.Join(columnNames, ", ")
}
