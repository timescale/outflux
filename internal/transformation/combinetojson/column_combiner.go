package combinetojson

import (
	"github.com/timescale/outflux/internal/idrf"
)

type columnCombiner interface {
	combine([]*idrf.ColumnInfo, map[string]bool, string) []*idrf.ColumnInfo
}

type defColCombiner struct{}

// combine takes an array of the original column definitions (originalColumns), a set
// of column names (columnNamesToReplace) which will be replaced with a single JSON type column,
// and resultColumn is the name of the new JSON column.
// The arguments have already been validated.
// resultColumnName column is placed at the position of the first column designated to be combined/replaced
func (d *defColCombiner) combine(
	originalColumns []*idrf.ColumnInfo,
	columnNamesToReplace map[string]bool,
	resultColumnName string) []*idrf.ColumnInfo {
	jsonColumnAdded := false
	numNewColumns := len(originalColumns) - len(columnNamesToReplace) + 1
	newColumns := make([]*idrf.ColumnInfo, numNewColumns)
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
