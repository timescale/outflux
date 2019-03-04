package pipeline

import (
	"log"

	"github.com/timescale/outflux/internal/extraction"

	"github.com/timescale/outflux/internal/ingestion"

	"github.com/timescale/outflux/internal/connections"
)

// Pipe connects an extractor and an ingestor
type Pipe interface {
	Run() error
	ID() string
}

type defPipe struct {
	id               string
	conf             *PipeConfig
	tsConnService    connections.TSConnectionService
	infConnService   connections.InfluxConnectionService
	ingestorService  ingestion.IngestorService
	extractorService extraction.ExtractorService
}

func (p *defPipe) ID() string {
	return p.id
}

func (p *defPipe) Run() error {
	// open connections
	influxConn, tsConn, err := p.openConnections()
	if err != nil {
		return err
	}

	// defer close connections
	defer influxConn.Close()
	defer tsConn.Close()

	// create ingestor and extractor
	extractor, ingestor, err := p.createElements(influxConn, tsConn)
	if err != nil {
		return err
	}

	// prepare them
	err = p.prepareElements(p.conf.connections, extractor, ingestor)
	if err != nil {
		return err
	}

	// run them
	if p.conf.extraction.MeasureExtraction.SchemaOnly {
		log.Printf("No data transfer will occur")
		return nil
	}

	return p.run(extractor, ingestor)
}
