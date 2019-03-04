package pipeline

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/extraction"
	"github.com/timescale/outflux/internal/ingestion"
)

func (p *defPipe) createElements(infConn influx.Client, tsConn *pgx.Conn) (extraction.Extractor, ingestion.Ingestor, error) {
	extractor, err := p.extractorService.InfluxExtractor(infConn, p.conf.extraction)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: could not create extractor\n%v", p.id, err)
	}

	ingestor := p.ingestorService.NewTimescaleIngestor(tsConn, p.conf.ingestion)
	return extractor, ingestor, nil
}
