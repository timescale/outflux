package pipeline

import (
	extractionConf "github.com/timescale/outflux/internal/extraction/config"
	ingestionConf "github.com/timescale/outflux/internal/ingestion/config"
)

type PipeConfig struct {
	extraction  *extractionConf.ExtractionConfig
	ingestion   *ingestionConf.IngestorConfig
	connections *ConnectionConfig
}
