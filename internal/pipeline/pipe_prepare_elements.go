package pipeline

import (
	"fmt"

	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
)

func (p *defPipe) prepareElements(connArgs *ConnectionConfig, extractor extraction.Extractor, ingestor ingestion.Ingestor) error {
	bundle, err := extractor.Prepare()
	if err != nil {
		return fmt.Errorf("%s: could not prepare extractor\n%v", p.id, err)
	}

	//rename retention policy to output schema
	_, measure := bundle.DataDef.SchemaAndTable()
	var newDSName string
	if connArgs.OutputSchema != "" {
		newDSName = connArgs.OutputSchema + "." + measure
	} else {
		newDSName = measure
	}
	bundle.DataDef.DataSetName = newDSName
	err = ingestor.Prepare(bundle)
	if err != nil {
		return fmt.Errorf("%s: could not prepare ingestor\n%v", p.id, err)
	}

	return nil
}
