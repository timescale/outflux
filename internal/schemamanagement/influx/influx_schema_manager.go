package influx

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type influxSchemaManager struct {
	measureExplorer    discovery.MeasureExplorer
	influxClient       influx.Client
	dataSetConstructor dataSetConstructor
	database           string
}

// NewInfluxSchemaManager creates new schema manager that can discover influx data sets
func NewInfluxSchemaManager(
	client influx.Client,
	queryService influxqueries.InfluxQueryService,
	db string) schemamanagement.SchemaManager {
	measureExplorer := discovery.NewMeasureExplorer(queryService)
	dsConstructor := newDataSetConstructor(db, client, queryService)
	return &influxSchemaManager{
		measureExplorer:    measureExplorer,
		influxClient:       client,
		dataSetConstructor: dsConstructor,
		database:           db,
	}
}

func (sm *influxSchemaManager) DiscoverDataSets() ([]string, error) {
	return sm.measureExplorer.FetchAvailableMeasurements(sm.influxClient, sm.database)
}

func (sm *influxSchemaManager) FetchDataSet(dataSetIdentifier string) (*idrf.DataSetInfo, error) {
	measurements, err := sm.measureExplorer.FetchAvailableMeasurements(sm.influxClient, sm.database)
	if err != nil {
		return nil, err
	}

	measureMissing := true
	for _, returnedMeasure := range measurements {
		if returnedMeasure == dataSetIdentifier {
			measureMissing = false
			break
		}
	}

	if measureMissing {
		return nil, fmt.Errorf("measure '%s' not found in database '%s'", dataSetIdentifier, sm.database)
	}

	return sm.dataSetConstructor.construct(dataSetIdentifier)
}

func (sm *influxSchemaManager) PrepareDataSet(dataSet *idrf.DataSetInfo, strategy schemamanagement.SchemaStrategy) error {
	panic(fmt.Errorf("not implemented"))
}
