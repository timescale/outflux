package extraction

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/utils"

	"github.com/timescale/outflux/schemadiscovery"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	"github.com/timescale/outflux/extraction/config"
)

func TestInfluxExtractorStart(t *testing.T) {
	measure := "measure"

	simpleConfig := config.Config{
		ExtractorID: "ID",
		MeasureExtraction: &config.MeasureExtraction{
			Database: "db", Measure: measure, ChunkSize: 1,
		},
		Connection: &clientutils.ConnectionParams{},
		DataSet:    &idrf.DataSetInfo{},
	}

	producer := &mockProducer{}

	extractor := defaultInfluxExtractor{
		config:   &simpleConfig,
		producer: producer,
	}

	res := extractor.Start(nil)

	if res == nil {
		t.Error("nil data channel returned")
	}

	// wait for channel to close at end of mocked method
	for range res {
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
	connectionParams *clientutils.ConnectionParams,
	database, measure string,
) (*idrf.DataSetInfo, error) {
	return me.resToReturn, me.errToReturn
}

func returnErrorOnMeasurementSchema() schemadiscovery.SchemaExplorer {
	mockMeasureExplorer := &mockMeasureExplorer{nil, fmt.Errorf("some error")}
	return schemadiscovery.NewSchemaExplorerWith(nil, mockMeasureExplorer)
}

func returnSchema(measure string, columnNames []string) schemadiscovery.SchemaExplorer {
	columns := make([]*idrf.ColumnInfo, len(columnNames))
	for i, columnName := range columnNames {
		columns[i], _ = idrf.NewColumn(columnName, idrf.IDRFBoolean)
	}

	twoColumnExample, _ := idrf.NewDataSet(measure, columns, columns[0].Name)
	mockMeasureExplorer := &mockMeasureExplorer{twoColumnExample, nil}
	return schemadiscovery.NewSchemaExplorerWith(nil, mockMeasureExplorer)
}

type mockProducer struct {
	numCalled int
}

func (dp *mockProducer) Fetch(connectionParams *clientutils.ConnectionParams,
	dataChannel chan idrf.Row,
	query influx.Query,
	errorBc utils.ErrorBroadcaster) {
	dp.numCalled++
	close(dataChannel)
}
