package influx

import (
	"fmt"
	"strings"

	"github.com/timescale/outflux/internal/extraction/config"
	"github.com/timescale/outflux/internal/idrf"
)

const (
	selectQueryDoubleBoundTemplate = "SELECT %s\nFROM \"%s\"\nWHERE time >= '%s' AND time <= '%s'"
	selectQueryLowerBoundTemplate  = "SELECT %s\nFROM \"%s\"\nWHERE time >= '%s'"
	selectQueryUpperBoundTemplate  = "SELECT %s\nFROM \"%s\"\nWHERE time <= '%s'"
	selectQueryNoBoundTemplate     = "SELECT %s\nFROM \"%s\""
	limitSuffixTemplate            = "\nLIMIT %d"
)

func buildSelectCommand(config *config.MeasureExtraction, columns []*idrf.ColumnInfo) string {
	projection := buildProjection(columns)
	var command string
	if config.From != "" && config.To != "" {
		command = fmt.Sprintf(selectQueryDoubleBoundTemplate, projection, config.Measure, config.From, config.To)
	} else if config.From != "" {
		command = fmt.Sprintf(selectQueryLowerBoundTemplate, projection, config.Measure, config.From)
	} else if config.To != "" {
		command = fmt.Sprintf(selectQueryUpperBoundTemplate, projection, config.Measure, config.To)
	} else {
		command = fmt.Sprintf(selectQueryNoBoundTemplate, projection, config.Measure)
	}

	if config.Limit == 0 {
		return command
	}

	limit := fmt.Sprintf(limitSuffixTemplate, config.Limit)
	return fmt.Sprintf("%s %s", command, limit)
}

func buildProjection(columns []*idrf.ColumnInfo) string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = fmt.Sprintf("\"%s\"", column.Name)
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
