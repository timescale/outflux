package schemamanagement

import (
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/schemaconfig"
)

// SchemaManager defines methods for schema discovery and preparation
type SchemaManager interface {
	DiscoverDataSets() ([]string, error)
	FetchDataSet(dataSetIdentifier string) (*idrf.DataSet, error)
	PrepareDataSet(*idrf.DataSet, schemaconfig.SchemaStrategy) error
}
