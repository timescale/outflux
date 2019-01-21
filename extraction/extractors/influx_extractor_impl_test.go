package extractors

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	"github.com/timescale/outflux/extraction/config"
)

func TestExtractingAMeasure(t *testing.T) {
	measureConfig, err := config.NewMeasureExtractionConfig(
		"benchmark",
		"cpu",
		"2018-01-01T00:00:00Z",
		"2018-01-01T00:00:10Z",
		10000)

	if err != nil {
		t.Error(err)
	}
	connParams := clientutils.ConnectionParams{
		Server:   "http://localhost:8086",
		Username: "test",
		Password: "test",
	}
	extractor := influxExtractorImpl{
		config:     measureConfig,
		connection: &connParams,
	}

	info, err := extractor.Start()
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Schema: ", info.dataSetSchema)
	for row := range info.dataChannel {
		fmt.Println(row)
	}

	// data channel is always closed when an error occurs
	channelError := <-info.errorChannel
	if channelError != nil {
		fmt.Println("error: " + channelError.Error())
	}
}
