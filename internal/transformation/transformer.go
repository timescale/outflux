package transformation

import "github.com/timescale/outflux/internal/idrf"

// Transformer takes a data channel of idrf.Rows and transformes them to different rows
type Transformer interface {
	// ID returns a string that identifies the transformer instance (all pipeline elements have it)
	ID() string
	// Prepare must be called before Start. It can be used to verify that the transformation is possible
	// Also, the input argument contains the data channel that will be consumed.
	// The returned bundle contains the data set definition after running the transformation and
	// a channel that will contain the transformed data
	Prepare(input *idrf.Bundle) (*idrf.Bundle, error)
	// Start consumes the data channel given in Prepare, transforms each Point/Row and feeds it to a channel
	// that was returned from Prepare
	Start(chan error) error
}
