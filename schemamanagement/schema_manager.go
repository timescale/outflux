package schemamanagement

import (
	"github.com/timescale/outflux/idrf"
)

type SchemaManager interface {
	DiscoverDataSets() ([]*idrf.DataSetInfo, error)
	FetchDataSet(schema, name string) (*idrf.DataSetInfo, error)
	PrepareDataSet(dataSet *idrf.DataSetInfo, strategy SchemaStrategy) error
}
