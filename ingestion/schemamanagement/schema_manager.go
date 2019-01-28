package schemamanagement

import (
	"database/sql"
	"fmt"

	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/ingestion/config"
)

type SchemaManager interface {
	Prepare(*PrepareArgs) error
}

// NewSchemaManager creates a new instance of the schema manager that prepares the outbound db to accept the data
func NewSchemaManager() SchemaManager {
	return &defaultManager{
		explorer: newSchemaExplorer(),
		dropper:  newTableDropper(),
		creator:  newTableCreator(),
	}
}

type defaultManager struct {
	explorer schemaExplorer
	dropper  tableDropper
	creator  tableCreator
}

// PrepareArgs houses all the arguments required to prepare an outbound db
type PrepareArgs struct {
	Strategy config.SchemaStrategy
	Schema   string
	DataSet  *idrf.DataSetInfo
	DbCon    *sql.DB
}

func (d *defaultManager) Prepare(args *PrepareArgs) error {
	tableExists, err := d.explorer.tableExists(args.DbCon, args.Schema, args.DataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not check if table exists. %s", err.Error())
	}

	strategyRequiresDrop := args.Strategy == config.DropAndCreate || args.Strategy == config.DropCascadeAndCreate
	if strategyRequiresDrop {
		return prepareWithDropStrategy(args, tableExists, d.dropper, d.creator)
	} else if args.Strategy == config.CreateIfMissing && !tableExists {
		return d.creator.Create(args.DbCon, args.Schema, args.DataSet)
	} else if args.Strategy != config.CreateIfMissing && args.Strategy != config.ValidateOnly {
		return fmt.Errorf("preparation step for strategy %v not implemented", args.Strategy)
	} else if args.Strategy == config.ValidateOnly && !tableExists {
		return fmt.Errorf("validate only strategy selected, but table is not created in db")
	}

	existingTableColumns, err := d.explorer.fetchTableColumns(args.DbCon, args.Schema, args.DataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not retreive column information for table %s", args.DataSet.DataSetName)
	}

	err = isExistingTableCompatible(existingTableColumns, args.DataSet.Columns, args.DataSet.TimeColumn)
	if err != nil {
		return fmt.Errorf("Existing table in target db is not compatible with required. %s", err.Error())
	}

	isHypertable, err := d.explorer.isHypertable(args.DbCon, args.Schema, args.DataSet.DataSetName)
	if err != nil {
		return fmt.Errorf("could not check if table %s is hypertable", args.DataSet.DataSetName)
	}

	if !isHypertable {
		return fmt.Errorf("existing table %s is not a hypertable", args.DataSet.DataSetName)
	}
	return nil
}

func prepareWithDropStrategy(args *PrepareArgs, tableExists bool, dropper tableDropper, creator tableCreator) error {
	if tableExists {
		cascade := args.Strategy == config.DropCascadeAndCreate
		err := dropper.Drop(args.DbCon, args.Schema, args.DataSet.DataSetName, cascade)
		if err != nil {
			return fmt.Errorf("selected schema strategy wanted to drop the existing table, but: %s", err.Error())
		}
	}

	return creator.Create(args.DbCon, args.Schema, args.DataSet)
}

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

func pgTypeToIdrf(pgType string) idrf.DataType {
	switch pgType {
	case "text":
		return idrf.IDRFString
	case "timestamp with time zone":
		return idrf.IDRFTimestamptz
	case "timestamp without time zone":
		return idrf.IDRFTimestamp
	case "double precision":
		return idrf.IDRFDouble
	case "integer":
		return idrf.IDRFInteger32
	case "bigint":
		return idrf.IDRFInteger64
	default:
		return idrf.IDRFUnknown
	}
}
