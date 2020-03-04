package influx

import (
	"fmt"
	"strings"

	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s FROM %s WHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s FROM %s WHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s FROM %s WHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s FROM %s"
	limitSuffixTemplate            = "LIMIT %d"
	measurementNameTemplate        = `"%s"`
	measurementNameWithRPTemplate  = `"%s"."%s"`
)

func buildSelectCommand(config *config.MeasureExtraction, columns []*idrf.Column) string {
	//projection := buildProjection(columns)
        projection := "*"
	measurementName := buildMeasurementName(config.RetentionPolicy, config.Measure)
	var command string
	if config.From != "" && config.To != "" {
		command = fmt.Sprintf(selectQueryDoubleBoundTemplate, projection, measurementName, config.From, config.To)
	} else if config.From != "" {
		command = fmt.Sprintf(selectQueryLowerBoundTemplate, projection, measurementName, config.From)
	} else if config.To != "" {
		command = fmt.Sprintf(selectQueryUpperBoundTemplate, projection, measurementName, config.To)
	} else {
		command = fmt.Sprintf(selectQueryNoBoundTemplate, projection, measurementName)
	}

	if config.Limit == 0 {
		return command
	}

	limit := fmt.Sprintf(limitSuffixTemplate, config.Limit)
	return fmt.Sprintf("%s %s", command, limit)
}

func buildMeasurementName(rp, measurement string) string {
	if rp != "" {
		return fmt.Sprintf(measurementNameWithRPTemplate, rp, measurement)
	}
	return fmt.Sprintf(measurementNameTemplate, measurement)
}

func buildProjection(columns []*idrf.Column) string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = fmt.Sprintf(`"%s"`, column.Name)
	}

	return strings.Join(columnNames, ", ")
}

func checkError(errorChannel chan error) error {
	select {
	case err := <-errorChannel:
		return err
	default:
		return nil
	}
}
