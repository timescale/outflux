package extraction

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"

	"github.com/timescale/outflux/schemadiscovery"

	"github.com/timescale/outflux/schemadiscovery/clientutils"

	"github.com/timescale/outflux/extraction/config"
)

func TestInfluxExtractorStart(t *testing.T) {
	measure := "measure"
	columnNames := []string{"a", "b"}

	simpleConfig := config.MeasureExtraction{
		Database: "db", Measure: measure, ChunkSize: 1,
	}

	producer := &mockProducer{}
	testCases := []struct {
		schemaExplorer schemadiscovery.SchemaExplorer
		config         config.MeasureExtraction
		producer       DataProducer
		expectedError  bool
	}{
		{schemaExplorer: returnErrorOnMeasurementSchema(), expectedError: true},
		{schemaExplorer: returnSchema(measure, columnNames), config: simpleConfig, producer: producer, expectedError: false},
	}

	for _, testCase := range testCases {
		extractor := defaultInfluxExtractor{
			schemaExplorer: testCase.schemaExplorer,
			config:         &testCase.config,
			producer:       testCase.producer,
		}

		res, err := extractor.Start()

		if err != nil && !testCase.expectedError {
			t.Errorf("no error expected, got: %v", err)
		}

		if err == nil && testCase.expectedError {
			t.Errorf("error was expected, none returned")
		}

		if testCase.expectedError {
			continue
		}

		mockedProducer := testCase.producer.(*mockProducer)

		if res.DataChannel == nil || res.ErrorChannel == nil || res.DataSetSchema == nil {
			t.Errorf("fetch method returned a nil value for some of the members of the result. result: %v", res)
		}

		// wait for channel to close at end of mocked method
		for range res.DataChannel {
		}

		if mockedProducer.numCalled == 0 {
			t.Errorf("fetch method not called")
		}
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
	errorChannel chan error,
	query influx.Query) {
	dp.numCalled++
	close(dataChannel)
}
