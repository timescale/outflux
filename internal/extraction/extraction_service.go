package extraction

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/extraction/config"
	influxExtraction "github.com/timescale/outflux/internal/extraction/influx"
	influxSchema "github.com/timescale/outflux/internal/schemamanagement/influx"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

// ExtractorService defines methods for creating extractor instances
type ExtractorService interface {
	InfluxExtractor(influx.Client, *config.ExtractionConfig) (Extractor, error)
}

// NewExtractorService creates a new instance of the service that can create extractors
func NewExtractorService(iqs influxqueries.InfluxQueryService) ExtractorService {
	return &extractorService{iqs}
}

type extractorService struct {
	influxQueryService influxqueries.InfluxQueryService
}

func (e *extractorService) InfluxExtractor(conn influx.Client, conf *config.ExtractionConfig) (Extractor, error) {
	err := config.ValidateMeasureExtractionConfig(conf.MeasureExtraction)
	if err != nil {
		return nil, fmt.Errorf("measure extraction config is not valid: %s", err.Error())
	}

	sm := influxSchema.NewInfluxSchemaManager(conn, e.influxQueryService, conf.MeasureExtraction.Database)
	dataProducer := influxExtraction.NewDataProducer(conf.ExtractorID, conn)
	return &influxExtraction.Extractor{
		Config:       conf,
		SM:           sm,
		DataProducer: dataProducer,
	}, nil
}
