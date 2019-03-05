package combinetojson

import (
	"github.com/timescale/outflux/internal/idrf"
)

type columnCombiner interface {
	combine([]*idrf.ColumnInfo, map[string]bool, string) []*idrf.ColumnInfo
}

type defColCombiner struct{}

// arguments have already been validated
// result column is placed at the position of the first column designated to be combined
func (d *defColCombiner) combine(columns []*idrf.ColumnInfo, toCombine map[string]bool, result string) []*idrf.ColumnInfo {
	jsonColumnAdded := false
	numNewColumns := len(columns) - len(toCombine) + 1
	newColumns := make([]*idrf.ColumnInfo, numNewColumns)
	currentColumn := 0
	for _, originalColumn := range columns {
		_, isCombinedColumn := toCombine[originalColumn.Name]
		if isCombinedColumn && !jsonColumnAdded {
			newColumns[currentColumn], _ = idrf.NewColumn(result, idrf.IDRFJson)
			jsonColumnAdded = true
		} else if isCombinedColumn && jsonColumnAdded {
			continue
		} else {
			newColumns[currentColumn] = originalColumn
		}

		currentColumn++
	}

	return newColumns
}
