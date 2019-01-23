package discovery

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

const (
	showTagsQueryTemplate = "SHOW TAG KEYS FROM %s"
)

// TagExplorer Defines an API for discovering the tags of an InfluxDB measurement
type TagExplorer interface {
	DiscoverMeasurementTags(influxClient influx.Client, database, measure string) ([]*idrf.ColumnInfo, error)
}

type defaultTagExplorer struct {
	utils clientutils.ClientUtils
}

// NewTagExplorer creates a new implementation of that can discover the tags of an influx measurement
func NewTagExplorer() TagExplorer {
	return &defaultTagExplorer{
		utils: clientutils.NewUtils(),
	}
}

// DiscoverMeasurementTags retrieves the tags for a given measurement and returns an IDRF representation for them.
func (te *defaultTagExplorer) DiscoverMeasurementTags(influxClient influx.Client, database, measure string) ([]*idrf.ColumnInfo, error) {
	tags, err := te.fetchMeasurementTags(influxClient, database, measure)

	if err != nil {
		return nil, err
	}

	return convertTags(tags)
}

func (te *defaultTagExplorer) fetchMeasurementTags(influxClient influx.Client, database, measure string) ([]string, error) {
	showTagsQuery := fmt.Sprintf(showTagsQueryTemplate, measure)
	result, err := te.utils.ExecuteShowQuery(influxClient, database, showTagsQuery)

	if err != nil {
		return nil, err
	}

	if len(result.Values) == 0 {
		return []string{}, nil
	}

	tagNames := make([]string, len(result.Values))
	for index, valuesRow := range result.Values {
		if len(valuesRow) != 1 {
			errorString := "tag discovery query returned unexpected result. " +
				"Tag names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		tagNames[index] = valuesRow[0]
	}

	return tagNames, nil
}

func convertTags(tags []string) ([]*idrf.ColumnInfo, error) {
	columns := make([]*idrf.ColumnInfo, len(tags))
	for i, tag := range tags {
		idrfColumn, err := idrf.NewColumn(tag, idrf.IDRFString)

		if err != nil {
			return nil, fmt.Errorf("Could not convert tags to IDRF. " + err.Error())
		}

		columns[i] = idrfColumn
	}

	return columns, nil
}
