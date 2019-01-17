package extractors

import (
	"github.com/timescale/outflux/idrf"
)

// InfluxExtractor defines an interface for an Extractor that can connect to an InfluxDB
// discover the schema and produces the rows to a channel
type InfluxExtractor interface {
	Start(rowChannel chan idrf.Row) (*idrf.DataSetInfo, error)
	Stop() error
}
