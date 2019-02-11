package ts

import (
	"fmt"

	"github.com/timescale/outflux/idrf"
)

func isExistingTableCompatible(existingColumns []*columnDesc, requiredColumns []*idrf.ColumnInfo, timeCol string) error {
	columnsByName := make(map[string]*columnDesc)
	for _, column := range existingColumns {
		columnsByName[column.columnName] = column
	}

	for _, reqColumn := range requiredColumns {
		colName := reqColumn.Name
		var existingCol *columnDesc
		var ok bool
		if existingCol, ok = columnsByName[colName]; !ok {
			return fmt.Errorf("Required column %s not found in existing table", colName)
		}

		existingType := pgTypeToIdrf(existingCol.dataType)
		if !existingType.CanFitInto(reqColumn.DataType) {
			return fmt.Errorf(
				"Required column %s of type %s is not compatible with existing type %s",
				colName, reqColumn.DataType, existingType)
		}

		// Only time column is allowed to have a NOT NULL constraint
		if !existingCol.isColumnNullable() && existingCol.columnName != timeCol {
			return fmt.Errorf("Existing column %s is not nullable. Can't guarantee data transfer", existingCol.columnName)
		}
	}

	return nil
}
