package ts

import (
	"errors"
	"testing"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"
	"github.com/timescale/outflux/internal/idrf"
)

func TestDataSetToSQLTableDef(t *testing.T) {
	singleCol := []*idrf.Column{{Name: "col1", DataType: idrf.IDRFTimestamp}}
	twoCols := []*idrf.Column{singleCol[0], {Name: "col2", DataType: idrf.IDRFDouble}}
	threeCols := []*idrf.Column{
		{Name: "col1", DataType: idrf.IDRFTimestamptz},
		{Name: "col2", DataType: idrf.IDRFString},
		{Name: "col 3", DataType: idrf.IDRFInteger64}}
	ds1, _ := idrf.NewDataSet("ds1", singleCol, singleCol[0].Name)
	ds2, _ := idrf.NewDataSet("ds2", twoCols, twoCols[0].Name)
	ds3, _ := idrf.NewDataSet("ds 3", threeCols, threeCols[0].Name)
	ds4, _ := idrf.NewDataSet("fake_schema.ds4", singleCol, singleCol[0].Name)
	tcs := []struct {
		ds       *idrf.DataSet
		schema   string
		expected string
	}{
		{ds: ds1, expected: "CREATE TABLE \"ds1\"(\"col1\" TIMESTAMP)"},
		{ds: ds2, expected: "CREATE TABLE \"ds2\"(\"col1\" TIMESTAMP, \"col2\" FLOAT)"},
		{ds: ds3, expected: "CREATE TABLE \"ds 3\"(\"col1\" TIMESTAMPTZ, \"col2\" TEXT, \"col 3\" BIGINT)"},
		{ds: ds4, schema: "schema", expected: "CREATE TABLE \"schema\".\"fake_schema.ds4\"(\"col1\" TIMESTAMP)"},
	}
	for _, tc := range tcs {
		query := dataSetToSQLTableDef(tc.schema, tc.ds)
		if query != tc.expected {
			t.Errorf("expected: %s\ngot: %s", tc.expected, query)
		}
	}
}

func TestCreateTable(t *testing.T) {
	genErr := errors.New("generic error")
	testCases := []struct {
		desc                string
		db                  *tcMockPgxW
		info                *idrf.DataSet
		expectErr           bool
		expectNumExecCalls  int
		expectNumQueryCalls int
	}{
		{
			desc: "error on exec create basic table",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{""},
				execErrs: []error{genErr},
			},
			info:               &idrf.DataSet{},
			expectErr:          true,
			expectNumExecCalls: 1,
		}, {
			desc: "error on create timescale extension",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{"", ""},
				execErrs: []error{nil, genErr},
			},
			info:               &idrf.DataSet{},
			expectErr:          true,
			expectNumExecCalls: 2,
		}, {
			desc: "error on create hypertable",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{"", "", ""},
				execErrs: []error{nil, nil, genErr},
			},
			info:               &idrf.DataSet{},
			expectErr:          true,
			expectNumExecCalls: 3,
		}, {
			desc: "all good",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{"", "", ""},
				execErrs: []error{nil, nil, genErr},
			},
			info:               &idrf.DataSet{},
			expectErr:          true,
			expectNumExecCalls: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c := &defaultTableCreator{}
			err := c.CreateTable(tc.db, tc.info)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectNumExecCalls, tc.db.currentExec)
			assert.Equal(t, tc.expectNumQueryCalls, tc.db.currentQ)
		})
	}
}

func TestUpdateMetadata(t *testing.T) {
	genErr := errors.New("generic error")
	metTabName := "meta table"
	testCases := []struct {
		desc                string
		db                  *tcMockPgxW
		expectErr           bool
		expectNumExecCalls  int
		expectNumQueryCalls int
		expectedQueries     []string
		expectedQueryArgs   [][]interface{}
		expectedExecs       []string
		expectedExecArgs    [][]interface{}
	}{
		{
			desc: "error on get metadata table",
			db: &tcMockPgxW{
				queryRes:  []*pgx.Rows{nil},
				queryErrs: []error{genErr},
			},
			expectErr:           true,
			expectNumQueryCalls: 1,
			expectedQueries:     []string{`SELECT EXISTS (SELECT 1 FROM "` + timescaleCatalogSchema + `"."` + metTabName + `" WHERE key = $1)`},
			expectedQueryArgs:   [][]interface{}{[]interface{}{metadataKey}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c := &defaultTableCreator{}
			err := c.UpdateMetadata(tc.db, metTabName)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			db := tc.db
			assert.Equal(t, tc.expectNumExecCalls, db.currentExec)
			assert.Equal(t, tc.expectNumQueryCalls, db.currentQ)
			assert.Equal(t, tc.expectedQueries, db.expQ)
			assert.Equal(t, tc.expectedQueryArgs, db.expQArgs)
			assert.Equal(t, tc.expectedExecs, db.expExec)
			assert.Equal(t, tc.expectedExecArgs, db.expExecArgs)
		})
	}
}

func TestCreateHypertable(t *testing.T) {
	genErr := errors.New("generic error")
	tabName := "table name"
	testCases := []struct {
		desc                string
		db                  *tcMockPgxW
		info                *idrf.DataSet
		schema              string
		chunkTimeInterval   string
		expectErr           bool
		expectNumExecCalls  int
		expectNumQueryCalls int
		expectedExecs       []string
	}{
		{
			desc:      "error on exec",
			expectErr: true,
			info: &idrf.DataSet{
				TimeColumn:  "tajm col",
				DataSetName: tabName},
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{""},
				execErrs: []error{genErr}},
			expectNumExecCalls: 1,
			expectedExecs:      []string{`SELECT create_hypertable('"` + tabName + `"', 'tajm col');`},
		}, {
			desc: "all good, no schema no chunk interval",
			info: &idrf.DataSet{
				TimeColumn:  "tajm col",
				DataSetName: tabName},
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{""},
				execErrs: []error{nil}},
			expectNumExecCalls: 1,
			expectedExecs:      []string{`SELECT create_hypertable('"` + tabName + `"', 'tajm col');`},
		}, {
			desc: "all good, has schema no chunk interval",
			info: &idrf.DataSet{
				TimeColumn:  "tajm col",
				DataSetName: tabName},
			schema: "she ma",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{""},
				execErrs: []error{nil}},
			expectNumExecCalls: 1,
			expectedExecs:      []string{`SELECT create_hypertable('"she ma"."` + tabName + `"', 'tajm col');`},
		}, {
			desc: "all good, has schema and chunk interval",
			info: &idrf.DataSet{
				TimeColumn:  "tajm col",
				DataSetName: tabName},
			schema:            "she ma",
			chunkTimeInterval: "1m",
			db: &tcMockPgxW{
				execRes:  []pgx.CommandTag{""},
				execErrs: []error{nil}},
			expectNumExecCalls: 1,
			expectedExecs:      []string{`SELECT create_hypertable('"she ma"."` + tabName + `"', 'tajm col', chunk_time_interval => interval '1m');`},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c := &defaultTableCreator{
				schema:            tc.schema,
				chunkTimeInterval: tc.chunkTimeInterval,
			}
			err := c.CreateHypertable(tc.db, tc.info)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			db := tc.db
			assert.Equal(t, tc.expectNumExecCalls, db.currentExec)
			assert.Equal(t, tc.expectNumQueryCalls, db.currentQ)
			assert.Equal(t, tc.expectedExecs, db.expExec)
		})
	}
}

type tcMockPgxW struct {
	execRes     []pgx.CommandTag
	execErrs    []error
	currentExec int
	queryRes    []*pgx.Rows
	queryErrs   []error
	currentQ    int
	expQ        []string
	expQArgs    [][]interface{}
	expExec     []string
	expExecArgs [][]interface{}
}

func (t *tcMockPgxW) Begin() (*pgx.Tx, error) { return nil, nil }
func (t *tcMockPgxW) CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error) {
	return 0, nil
}
func (t *tcMockPgxW) Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error) {
	if t.expExec == nil {
		t.expExec = make([]string, len(t.execRes))
		t.expExecArgs = make([][]interface{}, len(t.execRes))
	}
	tmp := t.currentExec
	t.expExec[tmp] = sql
	t.expExecArgs[tmp] = arguments
	t.currentExec++
	return t.execRes[tmp], t.execErrs[tmp]
}
func (t *tcMockPgxW) Query(sql string, args ...interface{}) (*pgx.Rows, error) {
	if t.expQ == nil {
		t.expQ = make([]string, len(t.queryRes))
		t.expQArgs = make([][]interface{}, len(t.queryRes))
	}
	tmp := t.currentQ
	t.expQ[tmp] = sql
	t.expQArgs[tmp] = args
	t.currentQ++
	return t.queryRes[tmp], t.queryErrs[tmp]
}
func (t *tcMockPgxW) Close() error {
	return nil
}
