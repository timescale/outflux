package cli

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/transformation"
	jsonCombiner "github.com/timescale/outflux/internal/transformation/jsoncombiner"
)

// TransformerService creates different transformers
type TransformerService interface {
	TagsAsJson(infConn influx.Client, id, db, measure string, resultCol string) (transformation.Transformer, error)
}

// NewTransformerService creates a new implementation of the TransformerService interface
func NewTransformerService(influxTagExplorer discovery.TagExplorer) TransformerService {
	return &transformerService{
		influxTagExplorer: influxTagExplorer,
	}
}

type transformerService struct {
	influxTagExplorer discovery.TagExplorer
}

func (t *transformerService) TagsAsJson(infConn influx.Client, id, db, measure string, resultCol string) (transformation.Transformer, error) {
	log.Printf("Tags for measure '%s' will be combined into a single JSONB column", measure)
	tags, err := t.fetchTags(infConn, db, measure)
	if err != nil {
		return nil, fmt.Errorf("could not create the transformer for measure '%s'\n%v", measure, err)
	}

	return jsonCombiner.NewTransformer(id, tags, resultCol)
}

func (t *transformerService) fetchTags(infConn influx.Client, db, measure string) ([]string, error) {
	tagsAsColumns, err := t.influxTagExplorer.DiscoverMeasurementTags(infConn, db, measure)
	if err != nil {
		return nil, err
	}

	tags := make([]string, len(tagsAsColumns))
	for i, tagColumn := range tagsAsColumns {
		tags[i] = tagColumn.Name
	}

	return tags, nil
}
