package cli

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/transformation"
)

const (
	transformerIDTemplate = "%s_transfomer_%s"
)

func (p *pipeService) createTransformers(pipeId string, infConn influx.Client, measure string, connConf *ConnectionConfig, conf *MigrationConfig) ([]transformation.Transformer, error) {
	transformers := []transformation.Transformer{}

	if conf.TagsAsJSON {
		id := fmt.Sprintf(transformerIDTemplate, pipeId, "tagsAsJSON")
		tagsTransformer, err := p.transformerService.TagsAsJSON(infConn, id, connConf.InputDb, measure, conf.TagsCol)
		if err != nil {
			return nil, err
		}
		// if measurement has not tags, a nil transformer is returned
		if tagsTransformer != nil {
			transformers = append(transformers, tagsTransformer)
		}
	}

	if conf.FieldsAsJSON {
		id := fmt.Sprintf(transformerIDTemplate, pipeId, "fieldsAsJSON")
		fieldsTransformer, err := p.transformerService.FieldsAsJSON(infConn, id, connConf.InputDb, measure, conf.FieldsCol)
		if err != nil {
			return nil, err
		}
		transformers = append(transformers, fieldsTransformer)
	}

	return transformers, nil
}
