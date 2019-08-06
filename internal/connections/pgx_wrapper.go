package connections

import "github.com/jackc/pgx"

// PgxWrap represents a wrapper interface around pgx.Conn, for easier testing.
type PgxWrap interface {
	Begin() (*pgx.Tx, error)
	CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error)
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
	Close() error
}

type defaultPgxWrapper struct {
	db *pgx.Conn
}

// NewPgxWrapper creates a new pgx.Conn wrapper.
func NewPgxWrapper(db *pgx.Conn) PgxWrap {
	return &defaultPgxWrapper{db}
}

func (d *defaultPgxWrapper) CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error) {
	return d.db.CopyFrom(tableName, columnNames, rowSrc)
}
func (d *defaultPgxWrapper) Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error) {
	return d.db.Exec(sql, arguments...)
}
func (d *defaultPgxWrapper) Query(sql string, args ...interface{}) (*pgx.Rows, error) {
	return d.db.Query(sql, args...)
}
func (d *defaultPgxWrapper) Close() error {
	return d.db.Close()
}
func (d *defaultPgxWrapper) Begin() (*pgx.Tx, error) {
	return d.db.Begin()
}
