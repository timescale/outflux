package ts

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/schemamanagement"
)

type prepareArgs struct {
	Strategy schemamanagement.SchemaStrategy
	DataSet  *idrf.DataSetInfo
}

func TestPrepareDataSetFails(t *testing.T) {
	exampleColumns := []*idrf.ColumnInfo{
		{Name: "time", DataType: idrf.IDRFTimestamptz},
		{Name: "a", DataType: idrf.IDRFString},
	}
	dataSet := &idrf.DataSetInfo{
		DataSetName: "ds",
		Columns:     exampleColumns,
		TimeColumn:  "time",
	}

	existingColumns := []*columnDesc{
		{"time", "timestamp with time zone", "NO"},
		{"a", "text", "YES"},
	}

	wrongExistingColumns := []*columnDesc{}

	testCases := []struct {
		args     prepareArgs
		explorer schemaExplorer
		creator  tableCreator
		desc     string
		strat    schemamanagement.SchemaStrategy
		dropper  tableDropper
	}{
		{
			args:     prepareArgs{DataSet: dataSet},
			explorer: errorOnTableExistsExplorer(),
			desc:     "error checking if target table exists",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropAndCreate},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			strat:    schemamanagement.DropAndCreate,
			desc:     "drop strategy, table doesn't exist, error on create",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropAndCreate},
			explorer: onTableExists(true),
			creator:  okOnTableCreate(),
			dropper:  errorOnDrop(),
			strat:    schemamanagement.DropAndCreate,
			desc:     "drop strategy, table exists, error on drop",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			desc:     "create if missing, table doesn't exist, error on table create",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onTableExists(false),
			desc:     "validate only strategy, table doesn't exist",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onFetchColError(),
			desc:     "validate only, table exists, error on fetch columns",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onFetchColWith(wrongExistingColumns),
			desc:     "validate only, incompatible tables",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onIsHypertableError(existingColumns),
			desc:     "validate only, compatible, error checking if hypertable",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: isNotHypertable(existingColumns),
			desc:     "validate only, compatible, but existing is not a hypertable",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onPartByError(existingColumns),
			desc:     "validate only, compatible, is hypertable, error checking partitioning",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: notPartitionedProperly(existingColumns),
			desc:     "validate only, compatible, is hypertable, partitioned by another column",
		},
	}

	for _, testC := range testCases {
		manager := &tsSchemaManager{
			explorer: testC.explorer,
			dropper:  testC.dropper,
			creator:  testC.creator,
		}

		err := manager.PrepareDataSet(testC.args.DataSet, testC.args.Strategy)
		if err == nil {
			t.Errorf("Expected an error, none received. Desc:%s", testC.desc)
		}
	}
}

func TestPrepareOk(t *testing.T) {
	exampleColumns := []*idrf.ColumnInfo{
		{Name: "time", DataType: idrf.IDRFTimestamptz},
		{Name: "a", DataType: idrf.IDRFString},
	}
	dataSet := &idrf.DataSetInfo{
		DataSetName: "ds",
		Columns:     exampleColumns,
		TimeColumn:  "time",
	}

	existingColumns := []*columnDesc{
		{"time", "timestamp with time zone", "NO"},
		{"a", "text", "YES"},
	}

	testCases := []struct {
		args     prepareArgs
		explorer schemaExplorer
		creator  tableCreator
		desc     string
		strat    schemamanagement.SchemaStrategy
		dropper  tableDropper
	}{
		{
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: properMock(existingColumns),
			desc:     "validate only, compatible",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: properMock(existingColumns),
			desc:     "create if missing, table exists, compatible",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: properMockForCreateIfMissing(existingColumns),
			creator:  okOnTableCreate(),
			desc:     "validate not called if need to create",
		},
	}

	for _, testC := range testCases {
		manager := tsSchemaManager{
			explorer: testC.explorer,
			dropper:  testC.dropper,
			creator:  testC.creator,
		}

		err := manager.PrepareDataSet(testC.args.DataSet, testC.args.Strategy)
		if err != nil {
			t.Errorf("Expected no error,\n received:%v\n. Desc:%s", err, testC.desc)
		}
	}
}

func errorOnTableExistsExplorer() schemaExplorer {
	errorTableFinder := &mocker{tableExistsR: false, tableExistsErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(errorTableFinder, nil, nil, nil)
}

func onTableExists(exists bool) schemaExplorer {
	finder := &mocker{tableExistsR: exists}
	return newSchemaExplorerWith(finder, nil, nil, nil)
}

func onFetchColError() schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, nil, nil)
}

func onFetchColWith(res []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: res}
	return newSchemaExplorerWith(mocker, mocker, nil, nil)
}

func onIsHypertableError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHypertableErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil)
}

func isNotHypertable(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: false}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil)
}

func onPartByError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: true, isTimePartErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker)
}

func notPartitionedProperly(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: true, isTimePartBy: false}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker)
}

func properMock(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: true, isTimePartBy: true}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker)
}

func properMockForCreateIfMissing(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: false, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, nil, nil)
}

func okOnTableCreate() tableCreator {
	return &mocker{}
}
func errorOnCreateTable() tableCreator {
	return &mocker{tableCreateError: fmt.Errorf("error")}
}

func errorOnDrop() tableDropper {
	return &mocker{dropError: fmt.Errorf("error")}
}

type mocker struct {
	tableExistsR     bool
	tableExistsErr   error
	tableCreateError error
	dropError        error
	fetchColError    error
	fetcColR         []*columnDesc
	isHypertableErr  error
	isHyper          bool
	isTimePartBy     bool
	isTimePartErr    error
}

func (m *mocker) tableExists(db *sql.DB, schemaName, tableName string) (bool, error) {
	return m.tableExistsR, m.tableExistsErr
}

func (m *mocker) fetchTableColumns(db *sql.DB, schemaName, tableName string) ([]*columnDesc, error) {
	return m.fetcColR, m.fetchColError
}

func (m *mocker) Create(dbConn *sql.DB, info *idrf.DataSetInfo) error {
	return m.tableCreateError
}

func (m *mocker) Drop(db *sql.DB, schema, table string, cascade bool) error {
	return m.dropError
}

func (m *mocker) isHypertable(db *sql.DB, schemaName, tableName string) (bool, error) {
	return m.isHyper, m.isHypertableErr
}

func (m *mocker) isTimePartitionedBy(db *sql.DB, schema, table, time string) (bool, error) {
	return m.isTimePartBy, m.isTimePartErr
}