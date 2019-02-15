package ts

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx"

	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/schemamanagement"
)

type prepareArgs struct {
	Strategy schemamanagement.SchemaStrategy
	DataSet  *idrf.DataSetInfo
}

func TestNewTSSchemaManager(t *testing.T) {
	NewTSSchemaManager(nil)
}

func TestDiscoverDataSets(t *testing.T) {
	sm := &tsSchemaManager{}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	sm.DiscoverDataSets()
	t.Errorf("The code did not panic")
}

func TestFetchDataSet(t *testing.T) {
	sm := &tsSchemaManager{}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	sm.FetchDataSet("", "")
	t.Errorf("The code did not panic")
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
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: errorOnTableExistsExplorer(),
			desc:     "error checking if target table exists",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropAndCreate},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			strat:    schemamanagement.DropAndCreate,
			desc:     "drop strategy, table doesn't exist, error on create",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropCascadeAndCreate},
			explorer: onTableExists(false),
			creator:  errorOnCreateTable(),
			strat:    schemamanagement.DropAndCreate,
			desc:     "drop cascade strategy, table doesn't exist, error on create",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropAndCreate},
			explorer: onTableExists(true),
			creator:  okOnTableCreate(),
			dropper:  errorOnDrop(),
			strat:    schemamanagement.DropAndCreate,
			desc:     "drop strategy, table exists, error on drop",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.DropAndCreate},
			explorer: onTableExists(true),
			creator:  okOnTableCreate(),
			dropper:  errorOnDrop(),
			strat:    schemamanagement.DropCascadeAndCreate,
			desc:     "drop cascade strategy, table exists, error on drop",
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
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onFetchColError(),
			desc:     "create if missing, table exists, error on fetch columns",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onFetchColWith(wrongExistingColumns),
			desc:     "validate only, incompatible tables",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onFetchColWith(wrongExistingColumns),
			desc:     "create if missing, incompatible tables",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onTsExistsError(existingColumns),
			desc:     "validate only, can't check if timescale extension is created",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onTsExistsError(existingColumns),
			desc:     "create if missing, can't check if timescale extension is created",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onTsNotExits(existingColumns),
			desc:     "validate only, timescale extension not created",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onIsHypertableError(existingColumns),
			desc:     "validate only, compatible, error checking if hypertable",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onIsHypertableError(existingColumns),
			desc:     "create if missing, compatible, error checking if hypertable",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: isNotHypertable(existingColumns),
			desc:     "validate only, compatible, but existing is not a hypertable",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.ValidateOnly},
			explorer: onPartByError(existingColumns),
			desc:     "validate only, compatible, is hypertable, error checking partitioning",
		}, {
			args:     prepareArgs{DataSet: dataSet, Strategy: schemamanagement.CreateIfMissing},
			explorer: onPartByError(existingColumns),
			desc:     "create if missing, compatible, is hypertable, error checking partitioning",
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
	return newSchemaExplorerWith(errorTableFinder, nil, nil, nil, nil)
}

func onTableExists(exists bool) schemaExplorer {
	finder := &mocker{tableExistsR: exists}
	return newSchemaExplorerWith(finder, nil, nil, nil, nil)
}

func onFetchColError() schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, nil, nil, nil)
}

func onFetchColWith(res []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: res}
	return newSchemaExplorerWith(mocker, mocker, nil, nil, nil)
}

func onTsExistsError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExtErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil, mocker)
}

func onTsNotExits(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: false}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil, mocker)
}
func onIsHypertableError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: true, isHypertableErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil, mocker)
}

func isNotHypertable(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: true, isHyper: false}
	return newSchemaExplorerWith(mocker, mocker, mocker, nil, mocker)
}

func onPartByError(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: true, isHyper: true, isTimePartErr: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker, mocker)
}

func notPartitionedProperly(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: true, isHyper: true, isTimePartBy: false}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker, mocker)
}

func properMock(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: true, fetcColR: cols, tsExt: true, isHyper: true, isTimePartBy: true}
	return newSchemaExplorerWith(mocker, mocker, mocker, mocker, mocker)
}

func properMockForCreateIfMissing(cols []*columnDesc) schemaExplorer {
	mocker := &mocker{tableExistsR: false, fetchColError: fmt.Errorf("error")}
	return newSchemaExplorerWith(mocker, mocker, nil, nil, nil)
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
	tsExt            bool
	tsExtErr         error
	isHypertableErr  error
	isHyper          bool
	isTimePartBy     bool
	isTimePartErr    error
}

func (m *mocker) tableExists(db *pgx.Conn, schemaName, tableName string) (bool, error) {
	return m.tableExistsR, m.tableExistsErr
}

func (m *mocker) fetchTableColumns(db *pgx.Conn, schemaName, tableName string) ([]*columnDesc, error) {
	return m.fetcColR, m.fetchColError
}

func (m *mocker) CreateTable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error {
	return m.tableCreateError
}

func (m *mocker) CreateHypertable(dbConn *pgx.Conn, info *idrf.DataSetInfo) error {
	return nil
}

func (m *mocker) CreateTimescaleExtension(dbConn *pgx.Conn) error {
	return nil
}

func (m *mocker) Drop(db *pgx.Conn, schema, table string, cascade bool) error {
	return m.dropError
}

func (m *mocker) isHypertable(db *pgx.Conn, schemaName, tableName string) (bool, error) {
	return m.isHyper, m.isHypertableErr
}

func (m *mocker) isTimePartitionedBy(db *pgx.Conn, schema, table, time string) (bool, error) {
	return m.isTimePartBy, m.isTimePartErr
}

func (m *mocker) timescaleExists(db *pgx.Conn) (bool, error) {
	return m.tsExt, m.tsExtErr
}
