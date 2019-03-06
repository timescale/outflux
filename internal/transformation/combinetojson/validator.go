package combinetojson

import (
	"fmt"

	"github.com/timescale/outflux/internal/idrf"
)

const (
	errResColumnEmptyFmt               = "%s: resulting column (after combination) can't be an empty string"
	errCombinedColumnIsDuplicateFmt    = "%s: naming combined columns '%s' will result in duplicate column names to exist in result data set"
	errTimeColumnCombinedFmt           = "%s: time column '%s' of origin data can't be combined in a JSON column"
	errUnknownColumnsForCombinationFmt = "%s: column to be combined '%s' not found in origin data set\nOrigin data set:%s"
)

type validator interface {
	validate(originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error
}

type defValidator struct {
	id string
}

// validate checks if the selected 'columnsToCombine' can be replaced by a JSON column named 'resCol'
// in the originData set
func (v *defValidator) validate(originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error {
	if resCol == "" {
		return fmt.Errorf(errResColumnEmptyFmt, v.id)
	}

	// To avoid duplicates, the combined column name cannot already exist UNLESS it is being combined "away"
	_, resColumnNamedAsACombinedColumn := columnsToCombine[resCol]
	if !resColumnNamedAsACombinedColumn && originData.ColumnNamed(resCol) != nil {
		return fmt.Errorf(errCombinedColumnIsDuplicateFmt, v.id, resCol)
	}

	if _, timeIsInCombined := columnsToCombine[originData.TimeColumn]; timeIsInCombined {
		return fmt.Errorf(errTimeColumnCombinedFmt, v.id, originData.TimeColumn)
	}

	for reqColumnName := range columnsToCombine {
		if originData.ColumnNamed(reqColumnName) == nil {
			return fmt.Errorf(errUnknownColumnsForCombinationFmt, v.id, reqColumnName, originData.String())
		}
	}

	return nil
}
