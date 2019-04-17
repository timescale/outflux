package influx

import (
	"fmt"

	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// SchemaManager implements the schemamanagement.SchemaManager interface
type SchemaManager struct {
	measureExplorer    discovery.MeasureExplorer
	influxClient       influx.Client
	dataSetConstructor dataSetConstructor
	database           string
}

// NewSchemaManager creates new schema manager that can discover influx data sets
func NewSchemaManager(client influx.Client, db string, iqs influxqueries.InfluxQueryService) *SchemaManager {
	measureExplorer := discovery.NewMeasureExplorer(iqs)
	dsConstructor := newDataSetConstructor(db, client, iqs)
	return &SchemaManager{
		measureExplorer:    measureExplorer,
		influxClient:       client,
		dataSetConstructor: dsConstructor,
		database:           db,
	}
}

// DiscoverDataSets returns a list of the available measurements in the connected
func (sm *SchemaManager) DiscoverDataSets() ([]string, error) {
	return sm.measureExplorer.FetchAvailableMeasurements(sm.influxClient, sm.database)
}

// FetchDataSet for a given data set identifier (retention.measureName, or just measureName)
// returns the idrf.DataSet describing it
func (sm *SchemaManager) FetchDataSet(dataSetIdentifier string) (*idrf.DataSet, error) {
	return sm.dataSetConstructor.construct(dataSetIdentifier)
}

// PrepareDataSet NOT IMPLEMENTED
func (sm *SchemaManager) PrepareDataSet(dataSet *idrf.DataSet, strategy schemaconfig.SchemaStrategy) error {
	panic(fmt.Errorf("not implemented"))
}
