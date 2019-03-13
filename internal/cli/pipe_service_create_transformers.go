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
	if !conf.TagsAsJSON {
		return []transformation.Transformer{}, nil
	}

	id := fmt.Sprintf(transformerIDTemplate, pipeId, "tagsAsJSON")
	transformer, err := p.transformerService.TagsAsJson(infConn, id, connConf.InputDb, measure, conf.TagsCol)
	if err != nil {
		return nil, err
	}

	return []transformation.Transformer{transformer}, nil
}
