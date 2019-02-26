package extraction

import (
	"fmt"
	"io"
	"log"

	"github.com/timescale/outflux/internal/extraction/idrfconversion"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/connections"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/utils"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s\nFROM \"%s\"\nWHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s\nFROM \"%s\"\nWHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s\nFROM \"%s\"\nWHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s\nFROM \"%s\""
	limitSuffixTemplate            = "\nLIMIT %d"
)

// DataProducer populates a data channel with the results from an influx query
type DataProducer interface {
	Fetch(connectionParams *connections.InfluxConnectionParams,
		dataChannel chan idrf.Row,
		query influx.Query,
		errorBroadcaster utils.ErrorBroadcaster)
}

type defaultDataProducer struct {
	extractorID             string
	influxConnectionService connections.InfluxConnectionService
	converter               idrfconversion.IdrfConverter
}

// NewDataProducer creates a new implementation of the data producer
func NewDataProducer(
	extractorID string,
	connectionService connections.InfluxConnectionService,
	converter idrfconversion.IdrfConverter) DataProducer {
	return &defaultDataProducer{
		extractorID:             extractorID,
		influxConnectionService: connectionService,
		converter:               converter,
	}
}

// Executes the select query and receives the chunked response, piping it to a data channel.
// If an error occurs a single error is sent to the error channel. Both channels are closed at the end of the routine.
func (dp *defaultDataProducer) Fetch(connectionParams *connections.InfluxConnectionParams,
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

	client, err := dp.influxConnectionService.NewConnection(connectionParams)

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

	totalRows := 0
	for {
		// Before requesting the next chunk, check if an error occured in some other goroutine
		if checkError(errorChannel) != nil {
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
		totalRows += len(rows.Values)
		log.Printf("%s: Extracted %d rows from Influx", dp.extractorID, totalRows)
		for _, valRow := range rows.Values {
			convertedRow, err := dp.converter.Convert(valRow)
			if err != nil {
				err = fmt.Errorf("extractor '%s': could not convert influx result to IDRF row\n%v", dp.extractorID, err)
				errorBroadcaster.Broadcast(dp.extractorID, err)
				return
			}

			dataChannel <- convertedRow
		}
	}
}
