package influx

import (
	"testing"

	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

func TestNewInfluxSchemaManager(t *testing.T) {
	client := &influxqueries.MockClient{}
	db := "db"
	NewInfluxSchemaManager(client, nil, db)
}
