package extraction

import (
	"testing"

	"github.com/timescale/outflux/connections"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/utils"
)

func TestInfluxExtractorStart(t *testing.T) {
	measure := "measure"

	simpleConfig := config.Config{
		ExtractorID: "ID",
		MeasureExtraction: &config.MeasureExtraction{
			Database: "db", Measure: measure, ChunkSize: 1,
		},
		Connection:  &connections.InfluxConnectionParams{},
		DataSet:     &idrf.DataSetInfo{},
		DataChannel: make(chan idrf.Row, 1),
	}

	producer := &mockProducer{}

	extractor := defaultInfluxExtractor{
		config:   &simpleConfig,
		producer: producer,
	}

	extractor.Start(nil)

	// wait for channel to close at end of mocked method
	for range simpleConfig.DataChannel {
	}

	if producer.numCalled == 0 {
		t.Errorf("fetch method not called")
	}
}

type mockMeasureExplorer struct {
	resToReturn *idrf.DataSetInfo
	errToReturn error
}

func (me *mockMeasureExplorer) InfluxMeasurementSchema(
	connectionParams *connections.InfluxConnectionParams,
	database, measure string,
) (*idrf.DataSetInfo, error) {
	return me.resToReturn, me.errToReturn
}

type mockProducer struct {
	numCalled int
}

func (dp *mockProducer) Fetch(connectionParams *connections.InfluxConnectionParams,
	dataChannel chan idrf.Row,
	query influx.Query,
	errorBc utils.ErrorBroadcaster) {
	dp.numCalled++
	close(dataChannel)
}
