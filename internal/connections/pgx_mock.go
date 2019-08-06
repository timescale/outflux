package connections

import "github.com/jackc/pgx"

// MockPgxW is a mock implementation of the PgxWrapper.
type MockPgxW struct {
	ExecRes         []pgx.CommandTag
	ExecErrs        []error
	CurrentExec     int
	QueryRes        []*pgx.Rows
	QueryErrs       []error
	CurrentQ        int
	ExpQ            []string
	ExpQArgs        [][]interface{}
	ExpExec         []string
	ExpExecArgs     [][]interface{}
	BeginRes        []*pgx.Tx
	BeginErr        []error
	CurrentBegin    int
	CopyFromErr     []error
	CurrentCopyFrom int
	ExpCopyFromTab  []pgx.Identifier
	ExpCopyFromCol  [][]string
}

// Begin opens a transaction.
func (t *MockPgxW) Begin() (*pgx.Tx, error) {
	tmp := t.CurrentBegin
	t.CurrentBegin++
	return t.BeginRes[tmp], t.BeginErr[tmp]
}

// CopyFrom uses COPY to insert data.
func (t *MockPgxW) CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error) {
	tmp := t.CurrentCopyFrom
	if t.ExpCopyFromTab == nil {
		t.ExpCopyFromTab = make([]pgx.Identifier, len(t.CopyFromErr))
		t.ExpCopyFromCol = make([][]string, len(t.CopyFromErr))
	}
	t.ExpCopyFromTab[tmp] = tableName
	t.ExpCopyFromCol[tmp] = columnNames
	t.CurrentCopyFrom++
	return 0, t.CopyFromErr[tmp]
}

// Exec executes an SQL statement, no results returned.
func (t *MockPgxW) Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error) {
	if t.ExpExec == nil {
		t.ExpExec = make([]string, len(t.ExecRes))
		t.ExpExecArgs = make([][]interface{}, len(t.ExecRes))
	}
	tmp := t.CurrentExec
	t.ExpExec[tmp] = sql
	t.ExpExecArgs[tmp] = arguments
	t.CurrentExec++
	return t.ExecRes[tmp], t.ExecErrs[tmp]
}

// Query data from the db.
func (t *MockPgxW) Query(sql string, args ...interface{}) (*pgx.Rows, error) {
	if t.ExpQ == nil {
		t.ExpQ = make([]string, len(t.QueryRes))
		t.ExpQArgs = make([][]interface{}, len(t.QueryRes))
	}
	tmp := t.CurrentQ
	t.ExpQ[tmp] = sql
	t.ExpQArgs[tmp] = args
	t.CurrentQ++
	return t.QueryRes[tmp], t.QueryErrs[tmp]
}

// Close the connection.
func (t *MockPgxW) Close() error {
	return nil
}
