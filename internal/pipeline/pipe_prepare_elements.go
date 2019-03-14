package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/transformation"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
)

func (p *defPipe) prepareElements(
	extractor extraction.Extractor,
	ingestor ingestion.Ingestor,
	transformers []transformation.Transformer) error {
	bundle, err := extractor.Prepare()
	if err != nil {
		return fmt.Errorf("%s: could not prepare extractor\n%v", p.id, err)
	}

	for _, transformer := range transformers {
		bundle, err = transformer.Prepare(bundle)
		if err != nil {
			return fmt.Errorf("%s: could not prepare transformer\n%v", p.id, err)
		}
	}

	err = ingestor.Prepare(bundle)
	if err != nil {
		return fmt.Errorf("%s: could not prepare ingestor\n%v", p.id, err)
	}
	return nil
}
