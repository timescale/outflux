package schemadiscovery

import (
	"fmt"

	influx "github.com/influxdata/platform/chronograf/influx"
	"github.com/timescale/outflux/idrf"
)

// DiscoverMeasurementTags retrieves the tags for a given measurement and returns an IDRF representation for them.
func DiscoverMeasurementTags(influxClient *influx.Client, database, measure string) ([]*idrf.ColumnInfo, error) {
	tags, err := fetchMeasurementTags(influxClient, database, measure)

	if err != nil {
		return nil, err
	}

	return convertTags(tags)
}

func fetchMeasurementTags(influxClient *influx.Client, database, measure string) ([]string, error) {
	showTagsQuery := fmt.Sprintf(showTagsQueryTemplate, measure)
	values, err := executeShowQuery(influxClient, database, showTagsQuery, acceptEmptyResultFlag)

	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return []string{}, nil
	}

	tagNames := make([]string, len(values))
	for index, valuesRow := range values {
		if len(valuesRow) != 1 {
			errorString := "tag discovery query returned unexpected result. " +
				"Tag names not represented in single column"
			return nil, fmt.Errorf(errorString)
		}

		tagNames[index] = valuesRow[0].(string)
	}

	return tagNames, nil
}

func convertTags(tags []string) ([]*idrf.ColumnInfo, error) {
	columns := make([]idrf.ColumnInfo, len(tags))
	for i, tag := range tags {
		idrfColumn, err := idrf.NewColumn(tag, idrf.IDRFString)

		if err != nil {
			return nil, fmt.Errorf("Could not convert tags to IDRF. " + err.Error())
		}

		columns[i] = idrfColumn
	}

	return columns, nil
}
