package cli

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	"github.com/timescale/outflux/internal/extraction"
	extrConfig "github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/ingestion"
	ingConfig "github.com/timescale/outflux/internal/ingestion/config"
)

func (p *pipeService) createElements(
	infConn influx.Client,
	tsConn *pgx.Conn,
	extrConf *extrConfig.ExtractionConfig,
	ingConf *ingConfig.IngestorConfig) (extraction.Extractor, ingestion.Ingestor, error) {
	extractor, err := p.extractorService.InfluxExtractor(infConn, extrConf)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create extractor\n%v", err)
	}

	ingestor := p.ingestorService.NewTimescaleIngestor(tsConn, ingConf)
	return extractor, ingestor, nil
}
