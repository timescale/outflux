package schemamanagement

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/timescale/outflux/idrf"
	"github.com/timescale/outflux/ingestion/config"
)

func TestPrepareFails(t *testing.T) {
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
		args     PrepareArgs
		explorer schemaExplorer
		creator  tableCreator
		desc     string
		strat    config.SchemaStrategy
		dropper  tableDropper
	}{
		{
			args:     PrepareArgs{DataSet: dataSet},
			explorer: errorOnTableExistsExplorer(),
			desc:     "error checking if target table exists",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.DropAndCreate},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			strat:    config.DropAndCreate,
			desc:     "drop strategy, table doesn't exist, error on create",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.DropAndCreate},
			explorer: onTableExists(true),
			creator:  okOnTableCreate(),
			dropper:  errorOnDrop(),
			strat:    config.DropAndCreate,
			desc:     "drop strategy, table exists, error on drop",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.CreateIfMissing},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			desc:     "create if missing, table doesn't exist, error on table create",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: onTableExists(false),
			desc:     "validate only strategy, table doesn't exist",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: onFetchColError(),
			desc:     "validate only, table exists, error on fetch columns",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: onFetchColWith(wrongExistingColumns),
			desc:     "validate only, incompatible tables",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: onIsHypertableError(existingColumns),
			desc:     "validate only, compatible, error checking if hypertable",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: isNotHypertable(existingColumns),
			desc:     "validate only, compatible, but existing is not a hypertable",
		},
	}

	for _, testC := range testCases {
		manager := defaultManager{
			explorer: testC.explorer,
			dropper:  testC.dropper,
			creator:  testC.creator,
		}

		err := manager.Prepare(&testC.args)
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
		args     PrepareArgs
		explorer schemaExplorer
		creator  tableCreator
		desc     string
		strat    config.SchemaStrategy
		dropper  tableDropper
	}{
		{
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.ValidateOnly},
			explorer: properMock(existingColumns),
			desc:     "validate only, compatible",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.CreateIfMissing},
			explorer: properMock(existingColumns),
			desc:     "validate only, compatible",
		}, {
			args:     PrepareArgs{DataSet: dataSet, Strategy: config.CreateIfMissing},
			explorer: properMockForCreateIfMissing(existingColumns),
			creator:  okOnTableCreate(),
			desc:     "validate not called if need to create",
		},
	}

	for _, testC := range testCases {
		manager := defaultManager{
			explorer: testC.explorer,
			dropper:  testC.dropper,
			creator:  testC.creator,
		}

		err := manager.Prepare(&testC.args)
		if err != nil {
			t.Errorf("Expected no error,\n received:%v\n. Desc:%s", err, testC.desc)
		}
	}
}

func errorOnTableExistsExplorer() schemaExplorer {
	errorTableFinder := &mocker{tableExistsR: false, tableExistsErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(errorTableFinder, nil, nil)
}

func onTableExists(exists bool) schemaExplorer {
	finder := &mocker{tableExistsR: exists}
	return newSchemaExplorerWith(finder, nil, nil)
}

func onFetchColError() schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, nil)
}

func onFetchColWith(res []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: res}
	return newSchemaExplorerWith(mocker, mocker, nil)
}

func onIsHypertableError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHypertableErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker)
}

func isNotHypertable(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: false}
	return newSchemaExplorerWith(mocker, mocker, mocker)
}

func properMock(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, isHyper: true}
	return newSchemaExplorerWith(mocker, mocker, mocker)
}

func properMockForCreateIfMissing(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: false, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker)
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
}

func (m *mocker) tableExists(db *sql.DB, schemaName, tableName string) (bool, error) {
	return m.tableExistsR, m.tableExistsErr
}

func (m *mocker) fetchTableColumns(db *sql.DB, schemaName, tableName string) ([]*columnDesc, error) {
	return m.fetcColR, m.fetchColError
}

func (m *mocker) Create(dbConn *sql.DB, schema string, info *idrf.DataSetInfo) error {
	return m.tableCreateError
}

func (m *mocker) Drop(db *sql.DB, schema, table string, cascade bool) error {
	return m.dropError
}

func (m *mocker) isHypertable(db *sql.DB, schemaName, tableName string) (bool, error) {
	return m.isHyper, m.isHypertableErr
}
