package influx

import (
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
	rp                 string
}

// NewSchemaManager creates new schema manager that can discover influx data sets
func NewSchemaManager(
	client influx.Client,
	db, rp string,
	me discovery.MeasureExplorer,
	tagExplorer discovery.TagExplorer,
	fieldExplorer discovery.FieldExplorer) *SchemaManager {
	dsConstructor := newDataSetConstructor(db, rp, client, tagExplorer, fieldExplorer)
	return &SchemaManager{
		measureExplorer:    me,
		influxClient:       client,
		dataSetConstructor: dsConstructor,
		database:           db,
		rp:                 rp,
	}
}

// DiscoverDataSets returns a list of the available measurements in the connected
func (sm *SchemaManager) DiscoverDataSets() ([]string, error) {
	return sm.measureExplorer.FetchAvailableMeasurements(sm.influxClient, sm.database, sm.rp)
}

// FetchDataSet for a given data set identifier (retention.measureName, or just measureName)
// returns the idrf.DataSet describing it
func (sm *SchemaManager) FetchDataSet(dataSetIdentifier string) (*idrf.DataSet, error) {
	return sm.dataSetConstructor.construct(dataSetIdentifier)
}

// PrepareDataSet NOT IMPLEMENTED
func (sm *SchemaManager) PrepareDataSet(dataSet *idrf.DataSet, strategy schemaconfig.SchemaStrategy) error {
	panic("not implemented")
}
