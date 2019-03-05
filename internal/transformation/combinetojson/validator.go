package combinetojson

import (
	"fmt"

	"github.com/timescale/outflux/internal/idrf"
)

type validator interface {
	validate(transformerID string, originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error
}

type defValidator struct {
}

func (v *defValidator) validate(transformerID string, originData *idrf.DataSet, resCol string, columnsToCombine map[string]bool) error {
	if resCol == "" {
		return fmt.Errorf("%s: resulting column (after combination) can't be an empty string", transformerID)
	}

	_, resColumnNamedAsACombinedColumn := columnsToCombine[resCol]
	if originData.ColumnNamed(resCol) != nil && !resColumnNamedAsACombinedColumn {
		return fmt.Errorf("%s: naming combined columns '%s' will result in duplicate column names to exist in result data set",
			transformerID, resCol)
	}

	if _, timeIsInCombined := columnsToCombine[originData.TimeColumn]; timeIsInCombined {
		errStr := "%s: time column '%s' of origin data can't be combined in a JSON column"
		return fmt.Errorf(errStr, transformerID, originData.TimeColumn)
	}

	for reqColumnName := range columnsToCombine {
		column := originData.ColumnNamed(reqColumnName)
		if column == nil {
			errStr := "%s: column to be combined '%s' not found in origin data set\nOrigin data set:%s"
			return fmt.Errorf(errStr, transformerID, reqColumnName, originData.String())
		}
	}

	return nil
}
