package extractors

import (
	"fmt"
	"io"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s FROM \"%s\" WHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s FROM \"%s\""
)

// NewInfluxExtractor creates an implementation of the InfluxExtractor interface while checking the arguments
func NewInfluxExtractor(config *config.MeasureExtraction, connection *clientutils.ConnectionParams) (InfluxExtractor, error) {
	if config == nil || connection == nil {
		return nil, fmt.Errorf("nil not allowed for config or connection")
	}

	return &influxExtractorImpl{config: config, connection: connection}, nil
}

// InfluxExtractorImpl is an implementation of the extractor interface.
type influxExtractorImpl struct {
	config     *config.MeasureExtraction
	connection *clientutils.ConnectionParams
}

// Start returns the schema info for a Influx Measurement and produces the the points as IDRFRows
// to a supplied channel
func (ie *influxExtractorImpl) Start() (*ExtractedInfo, error) {

	dataSetInfo, err := schemadiscovery.InfluxMeasurementSchema(ie.connection, ie.config.Database, ie.config.Measure)
	if err != nil {
		return nil, err
	}

	query := influx.Query{
		Command:   buildSelectCommand(ie.config, dataSetInfo.Columns),
		Database:  ie.config.Database,
		Chunked:   true,
		ChunkSize: ie.config.ChunkSize,
	}

	dataChannel := make(chan idrf.Row)
	errorChannel := make(chan error)

	go fetchData(dataChannel, errorChannel, ie.connection, query)

	return &ExtractedInfo{
		dataSetSchema: dataSetInfo,
		dataChannel:   dataChannel,
		errorChannel:  errorChannel,
	}, nil
}

// Goroutine that executes the select query and receives the chunked response, piping it to a data channel.
// If an error occurs a single error is sent to the error channel. Both channels are closed at the end of the routine.
func fetchData(
	dataChannel chan idrf.Row,
	errorChannel chan error,
	connection *clientutils.ConnectionParams,
	query influx.Query) {

	defer close(dataChannel)
	defer close(errorChannel)

	client, err := clientutils.CreateInfluxClient(connection)

	if err != nil {
		errorChannel <- err
		return
	}

	defer client.Close()

	chunkResponse, err := client.QueryAsChunk(query)
	if err != nil {
		errorChannel <- err
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
