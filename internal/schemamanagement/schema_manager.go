package schemamanagement

import "github.com/timescale/outflux/internal/idrf"

type SchemaManager interface {
	DiscoverDataSets() ([]string, error)
	FetchDataSet(dataSetIdentifier string) (*idrf.DataSetInfo, error)
	PrepareDataSet(dataSet *idrf.DataSetInfo, strategy SchemaStrategy) error
}
