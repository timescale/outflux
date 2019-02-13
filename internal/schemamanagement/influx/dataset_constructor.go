package influx

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
	"github.com/timescale/outflux/internal/schemamanagement/influx/influxqueries"
)

type dataSetConstructor interface {
	construct(measure string) (*idrf.DataSetInfo, error)
}

func newDataSetConstructor(db string, client influx.Client, queryService influxqueries.InfluxQueryService) dataSetConstructor {
	return &defaultDSConstructor{
		database:      db,
		influxClient:  client,
		tagExplorer:   discovery.NewTagExplorer(queryService),
		fieldExplorer: discovery.NewFieldExplorer(queryService),
	}
}

type defaultDSConstructor struct {
	database      string
	tagExplorer   discovery.TagExplorer
	fieldExplorer discovery.FieldExplorer
	influxClient  influx.Client
}

func (d *defaultDSConstructor) construct(measure string) (*idrf.DataSetInfo, error) {
	idrfTags, err := d.tagExplorer.DiscoverMeasurementTags(d.influxClient, d.database, measure)
	if err != nil {
		return nil, err
	}

	idrfFields, err := d.fieldExplorer.DiscoverMeasurementFields(d.influxClient, d.database, measure)
	if err != nil {
		return nil, err
	}

	idrfTimeColumn, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	allColumns := []*idrf.ColumnInfo{idrfTimeColumn}
	allColumns = append(allColumns, idrfTags...)
	allColumns = append(allColumns, idrfFields...)
	dataSet, err := idrf.NewDataSet("", measure, allColumns, "time")
	return dataSet, err
}
