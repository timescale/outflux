package schemamanagement

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/jackc/pgx"
	influxSchema "github.com/timescale/outflux/internal/schemamanagement/influx"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
	tsSchema "github.com/timescale/outflux/internal/schemamanagement/ts"
)

// SchemaManagerService defines methods for creating SchemaManagers
type SchemaManagerService interface {
	Influx(client influx.Client, db string) SchemaManager
	TimeScale(dbConn *pgx.Conn) SchemaManager
}

// NewSchemaManagerService returns an instance of SchemaManagerService
func NewSchemaManagerService(influxQueryService influxqueries.InfluxQueryService) SchemaManagerService {
	return &schemaManagerService{
		influxQueryService,
	}
}

type schemaManagerService struct {
	influxQueryService influxqueries.InfluxQueryService
}

// Influx creates new schema manager that can discover influx data sets
func (s *schemaManagerService) Influx(client influx.Client, db string) SchemaManager {
	return influxSchema.NewSchemaManager(client, db, s.influxQueryService)
}

func (s *schemaManagerService) TimeScale(dbConn *pgx.Conn) SchemaManager {
	return tsSchema.NewTSSchemaManager(dbConn)
}
