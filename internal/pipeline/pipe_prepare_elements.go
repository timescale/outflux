package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
)

func (p *defPipe) prepareElements(extractor extraction.Extractor, ingestor ingestion.Ingestor) error {
	bundle, err := extractor.Prepare()
	if err != nil {
		return fmt.Errorf("%s: could not prepare extractor\n%v", p.id, err)
	}
	err = ingestor.Prepare(bundle)
	if err != nil {
		return fmt.Errorf("%s: could not prepare ingestor\n%v", p.id, err)
	}
	return nil
}
