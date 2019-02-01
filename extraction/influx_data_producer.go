package extraction

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/timescale/outflux/utils"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s FROM \"%s\" WHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s FROM \"%s\" WHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s FROM \"%s\""
	limitSuffixTemplate            = "LIMIT %d"
)

// DataProducer populates a data channel with the results from an influx query
type DataProducer interface {
	Fetch(connectionParams *clientutils.ConnectionParams,
		dataChannel chan idrf.Row,
		query influx.Query,
		errorBroadcaster utils.ErrorBroadcaster)
}

type defaultDataProducer struct {
	extractorID string
	influxUtils clientutils.ClientUtils
}

// NewDataProducer creates a new implementation of the data producer
func NewDataProducer(extractorID string) DataProducer {
	return &defaultDataProducer{
		extractorID: extractorID,
		influxUtils: clientutils.NewUtils(),
	}
}

// NewDataProducerWith creates a new implementation of the data producer with a supplied client utils
func NewDataProducerWith(extractorID string, influxUtils clientutils.ClientUtils) DataProducer {
	return &defaultDataProducer{
		extractorID: extractorID,
		influxUtils: influxUtils,
	}
}

// Executes the select query and receives the chunked response, piping it to a data channel.
// If an error occurs a single error is sent to the error channel. Both channels are closed at the end of the routine.
func (dp *defaultDataProducer) Fetch(connectionParams *clientutils.ConnectionParams,
	dataChannel chan idrf.Row,
	query influx.Query,
	errorBroadcaster utils.ErrorBroadcaster) {
	defer close(dataChannel)

	errorChannel, err := errorBroadcaster.Subscribe(dp.extractorID)
	if err != nil {
		err = fmt.Errorf("extractor '%s' couldn't subscribe for errors", dp.extractorID)
		errorBroadcaster.Broadcast(dp.extractorID, err)
		return
	}

	defer errorBroadcaster.Unsubscribe(dp.extractorID)

	client, err := dp.influxUtils.CreateInfluxClient(connectionParams)

	if err != nil {
		err = fmt.Errorf("extractor '%s' couldn't create an influx client.\n%v", dp.extractorID, err)
		errorBroadcaster.Broadcast(dp.extractorID, err)
		return
	}

	defer client.Close()

	chunkResponse, err := client.QueryAsChunk(query)
	if err != nil {
		err = fmt.Errorf("extractor '%s' could not execute a chunked query.\n%v", dp.extractorID, err)
		log.Printf("'%s': %v", dp.extractorID, err)
		errorBroadcaster.Broadcast(dp.extractorID, err)
		return
	}

	defer chunkResponse.Close()

	for {
		// Before requesting the next chunk, check if an error occured in some other goroutine
		errorNotification := checkError(errorChannel)
		if errorNotification != nil {
			return
		}

		response, err := chunkResponse.NextResponse()
		if err != nil {
			if err == io.EOF {
				return
			}

			// If we got an error while decoding the response, send that back.
			err = fmt.Errorf("extractor '%s': error decoding response.\n%v", dp.extractorID, err)
			errorBroadcaster.Broadcast(dp.extractorID, err)
			return
		}

		if response == nil || response.Err != "" || len(response.Results) != 1 {
			err = fmt.Errorf("extractor '%s': server did not return a proper response", dp.extractorID)
			errorBroadcaster.Broadcast(dp.extractorID, err)
			return
		}

		series := response.Results[0].Series
		if len(series) > 1 {
			err = fmt.Errorf("extractor '%s': returned response had an unexpected format", dp.extractorID)
			errorBroadcaster.Broadcast(dp.extractorID, err)
			return
		} else if len(series) == 0 {
			return
		}

		rows := series[0]
		log.Printf("%s: Fetched %d new rows from Influx", dp.extractorID, len(rows.Values))
		for _, valRow := range rows.Values {
			dataChannel <- valRow
		}
	}
}

func buildSelectCommand(config *config.MeasureExtraction, columns []*idrf.ColumnInfo) string {
	projection := buildProjection(columns)
	var command string
	if config.From != "" && config.To != "" {
		command = fmt.Sprintf(selectQueryDoubleBoundTemplate, projection, config.Measure, config.From, config.To)
	} else if config.From != "" {
		command = fmt.Sprintf(selectQueryLowerBoundTemplate, projection, config.Measure, config.From)
	} else if config.To != "" {
		command = fmt.Sprintf(selectQueryUpperBoundTemplate, projection, config.Measure, config.To)
	} else {
		command = fmt.Sprintf(selectQueryNoBoundTemplate, projection, config.Measure)
	}

	if config.Limit == 0 {
		return command
	}

	limit := fmt.Sprintf(limitSuffixTemplate, config.Limit)
	return fmt.Sprintf("%s %s", command, limit)
}

func buildProjection(columns []*idrf.ColumnInfo) string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = fmt.Sprintf("\"%s\"", column.Name)
	}

	return strings.Join(columnNames, ", ")
}

func checkError(errorChannel chan error) error {
	select {
	case err := <-errorChannel:
		return err
	default:
		return nil
	}
}
