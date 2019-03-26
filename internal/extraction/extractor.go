package extraction

import (
	"github.com/timescale/outflux/internal/idrf"
)

// Extractor defines an interface for pulling data out of a database.
// When Prepare is called a data channel with a description of the
// data is returned. On Start the data channel is populated.
type Extractor interface {
	ID() string
	Prepare() (*idrf.Bundle, error)
	Start(chan error) error
}
