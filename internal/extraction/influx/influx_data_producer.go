package influx

import (
	"fmt"
	"io"
	"log"

	"github.com/timescale/outflux/internal/extraction/influx/idrfconversion"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
)

// DataProducer populates a data channel with the results from an influx query
type DataProducer interface {
	Fetch(*producerArgs) error
}

// NewDataProducer craetes a new DataProducer
func NewDataProducer(id string, influxClient influx.Client) DataProducer {
	return &defaultDataProducer{
		id, influxClient,
	}
}

type defaultDataProducer struct {
	extractorID  string
	influxClient influx.Client
}

type producerArgs struct {
	dataChannel chan idrf.Row
	errChannel  chan error
	query       *influx.Query
	converter   idrfconversion.IdrfConverter
}

// Executes the select query and receives the chunked response, piping it to a data channel.
// If an error occurs a single error is sent to the error channel. Both channels are closed at the end of the routine.
func (dp *defaultDataProducer) Fetch(args *producerArgs) error {
	defer close(args.dataChannel)

	chunkResponse, err := dp.influxClient.QueryAsChunk(*args.query)
	if err != nil {
		err = fmt.Errorf("extractor '%s' could not execute a chunked query.\n%v", dp.extractorID, err)
		log.Printf("'%s': %v", dp.extractorID, err)
		return err
	}

	defer chunkResponse.Close()

	totalRows := 0
	for {
		// Before requesting the next chunk, check if an error occurred in some other goroutine
		if err = checkError(args.errChannel); err != nil {
			return nil
		}

		response, err := chunkResponse.NextResponse()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			// If we got an error while decoding the response, send that back.
			err = fmt.Errorf("extractor '%s': error decoding response.\n%v", dp.extractorID, err)
			return err
		}

		if response == nil || response.Err != "" || len(response.Results) != 1 {
			return fmt.Errorf("extractor '%s': server did not return a proper response", dp.extractorID)
		}

		series := response.Results[0].Series
		if len(series) > 1 {
			return fmt.Errorf("extractor '%s': returned response had an unexpected format", dp.extractorID)
		} else if len(series) == 0 {
			return nil
		}

		rows := series[0]
		totalRows += len(rows.Values)
		log.Printf("%s: Extracted %d rows from Influx", dp.extractorID, totalRows)
		for _, valRow := range rows.Values {
			convertedRow, err := args.converter.Convert(valRow)
			if err != nil {
				return fmt.Errorf("extractor '%s': could not convert influx result to IDRF row\n%v", dp.extractorID, err)
			}

			args.dataChannel <- convertedRow
		}
	}

}
