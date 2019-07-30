package influx

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement/influx/discovery"
)

// DataSetConstructor builds a idrf.DataSet for a given measure
type dataSetConstructor interface {
	construct(measure string) (*idrf.DataSet, error)
}

// NewDataSetConstructor creates a new instance of a DataSetConstructor
func newDataSetConstructor(
	db, rp string, onConflictConvertIntToFloat bool,
	client influx.Client,
	tagExplorer discovery.TagExplorer,
	fieldExplorer discovery.FieldExplorer) dataSetConstructor {
	return &defaultDSConstructor{
		database:                    db,
		rp:                          rp,
		influxClient:                client,
		tagExplorer:                 tagExplorer,
		fieldExplorer:               fieldExplorer,
		onConflictConvertIntToFloat: onConflictConvertIntToFloat,
	}
}

type defaultDSConstructor struct {
	database                    string
	rp                          string
	onConflictConvertIntToFloat bool
	tagExplorer                 discovery.TagExplorer
	fieldExplorer               discovery.FieldExplorer
	influxClient                influx.Client
}

func (d *defaultDSConstructor) construct(measure string) (*idrf.DataSet, error) {
	idrfTags, err := d.tagExplorer.DiscoverMeasurementTags(d.influxClient, d.database, d.rp, measure)
	if err != nil {
		return nil, fmt.Errorf("could not discover the tags of measurement '%s'\n%v", measure, err)
	}

	idrfFields, err := d.fieldExplorer.DiscoverMeasurementFields(d.influxClient, d.database, d.rp, measure, d.onConflictConvertIntToFloat)
	if err != nil {
		return nil, fmt.Errorf("could not discover the fields of measure '%s'\n%v", measure, err)
	}

	idrfTimeColumn, _ := idrf.NewColumn("time", idrf.IDRFTimestamp)
	allColumns := []*idrf.Column{idrfTimeColumn}
	allColumns = append(allColumns, idrfTags...)
	allColumns = append(allColumns, idrfFields...)
	dataSet, err := idrf.NewDataSet(measure, allColumns, "time")
	return dataSet, err
}
