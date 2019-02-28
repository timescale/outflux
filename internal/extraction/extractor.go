package extraction

import (
	"github.com/timescale/outflux/internal/idrf"
)

type Extractor interface {
	ID() string
	Prepare() (*idrf.Bundle, error)
	Start(chan error) error
}
