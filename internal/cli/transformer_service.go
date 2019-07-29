package cli

import (
	"fmt"
	"log"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/transformation"
	jsonCombiner "github.com/timescale/outflux/internal/transformation/jsoncombiner"
)

// TransformerService creates different transformers
type TransformerService interface {
	TagsAsJSON(infConn influx.Client, id, db, rp, measure string, resultCol string) (transformation.Transformer, error)
	FieldsAsJSON(infConn influx.Client, id, db, rp, measure string, resultCol string) (transformation.Transformer, error)
}

// NewTransformerService creates a new implementation of the TransformerService interface
func NewTransformerService(influxTagExplorer discovery.TagExplorer, influxFieldExplorer discovery.FieldExplorer) TransformerService {
	return &transformerService{
		influxTagExplorer:   influxTagExplorer,
		influxFieldExplorer: influxFieldExplorer,
	}
}

type transformerService struct {
	influxTagExplorer   discovery.TagExplorer
	influxFieldExplorer discovery.FieldExplorer
}

// TagsAsJSON returns a transformer that combines the tags into a single JSONb column.
// Returns a Transformer instance or nil if there are no tags.
// Returns an error if the tags couldn't be discovered or the instance of the transformer
// could not be created.
func (t *transformerService) TagsAsJSON(infConn influx.Client, id, db, rp, measure string, resultCol string) (transformation.Transformer, error) {
	log.Printf("Tags for measure '%s' will be combined into a single JSONB column", measure)
	tags, err := t.fetchTags(infConn, db, rp, measure)
	if err != nil {
		return nil, fmt.Errorf("could not create the transformer for measure '%s'\n%v", measure, err)
	}

	if len(tags) == 0 {
		log.Printf("%s: measure '%s' doesn't have any tags, will not be transformed", id, measure)
		return nil, nil
	}
	return jsonCombiner.NewTransformer(id, tags, resultCol)
}

// FieldsAsJSON returns a transformer that combines the fields into a single JSONb column.
func (t *transformerService) FieldsAsJSON(infConn influx.Client, id, db, rp, measure string, resultCol string) (transformation.Transformer, error) {
	log.Printf("Fields for measure '%s' will be combined into a single JSONB column", measure)
	fields, err := t.fetchFields(infConn, db, rp, measure)
	if err != nil {
		return nil, fmt.Errorf("could not create the transformer for measure '%s'\n%v", measure, err)
	}

	return jsonCombiner.NewTransformer(id, fields, resultCol)
}

type fetchColumnsFn func() ([]*idrf.Column, error)

func (t *transformerService) fetchTags(infConn influx.Client, db, rp, measure string) ([]string, error) {
	fetchFn := func() ([]*idrf.Column, error) {
		return t.influxTagExplorer.DiscoverMeasurementTags(infConn, db, rp, measure)
	}

	return fetch(fetchFn)
}

func (t *transformerService) fetchFields(infConn influx.Client, db, rp, measure string) ([]string, error) {
	fetchFn := func() ([]*idrf.Column, error) {
		return t.influxFieldExplorer.DiscoverMeasurementFields(infConn, db, rp, measure)
	}

	return fetch(fetchFn)
}

func fetch(fetch fetchColumnsFn) ([]string, error) {
	columns, err := fetch()
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	return columnNames, nil
}
