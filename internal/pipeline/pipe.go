package pipeline

import (
	"log"

	"github.com/timescale/outflux/internal/transformation"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
)

// Pipe connects an extractor and an ingestor
type Pipe interface {
	Run() error
	ID() string
}

// NewPipe creates an implementation of the Pipe interface
func NewPipe(id string, ing ingestion.Ingestor, ext extraction.Extractor, trans []transformation.Transformer, prepareOnly bool) Pipe {
	return &defPipe{
		id, ing, ext, trans, prepareOnly,
	}
}

type defPipe struct {
	id           string
	ingestor     ingestion.Ingestor
	extractor    extraction.Extractor
	transformers []transformation.Transformer
	prepareOnly  bool
}

func (p *defPipe) ID() string {
	return p.id
}

func (p *defPipe) Run() error {
	// prepare elements
	err := p.prepareElements(p.extractor, p.ingestor, p.transformers)
	if err != nil {
		return err
	}

	if p.prepareOnly {
		log.Printf("No data transfer will occur")
		return nil
	}

	// run them
	return p.run(p.extractor, p.ingestor, p.transformers)
}
