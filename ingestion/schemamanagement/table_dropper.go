package schemamanagement

import (
	"database/sql"
	"fmt"
)

const (
	dropTableQueryTemplate        = "DROP TABLE %s"
	dropTableCascadeQueryTemplate = "DROP TABLE %s CASCADE"
)

type tableDropper interface {
	Drop(db *sql.DB, schema, table string, cascade bool) error
}

type defaultTableDropper struct{}

func newTableDropper() tableDropper {
	return &defaultTableDropper{}
}
func (d *defaultTableDropper) Drop(db *sql.DB, schema, table string, cascade bool) error {
	name := table
	if schema != "" {
		name = schema + "." + name
	}

	var query string
	if cascade {
		query = fmt.Sprintf(dropTableCascadeQueryTemplate, name)
	} else {
		query = fmt.Sprintf(dropTableQueryTemplate, name)
	}

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}
