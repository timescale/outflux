package jsoncombiner

import (
	"github.com/timescale/outflux/internal/idrf"
)

type columnCombiner interface {
	combine([]*idrf.Column, map[string]bool, string) []*idrf.Column
}

type defColCombiner struct{}

// combine takes an array of the original column definitions (originalColumns), a set
// of column names (columnNamesToReplace) which will be replaced with a single JSON type column,
// and resultColumn is the name of the new JSON column.
// The arguments have already been validated.
// resultColumnName column is placed at the position of the first column designated to be combined/replaced
func (d *defColCombiner) combine(
	originalColumns []*idrf.Column,
	columnNamesToReplace map[string]bool,
	resultColumnName string) []*idrf.Column {
	jsonColumnAdded := false
	numNewColumns := len(originalColumns) - len(columnNamesToReplace) + 1
	newColumns := make([]*idrf.Column, numNewColumns)
	currentColumn := 0
	for _, originalColumn := range originalColumns {
		_, shouldReplaceColumn := columnNamesToReplace[originalColumn.Name]
		if shouldReplaceColumn && !jsonColumnAdded {
			newColumns[currentColumn], _ = idrf.NewColumn(resultColumnName, idrf.IDRFJson)
			jsonColumnAdded = true
		} else if shouldReplaceColumn && jsonColumnAdded {
			continue
		} else {
			newColumns[currentColumn] = originalColumn
		}

		currentColumn++
	}

	return newColumns
}
