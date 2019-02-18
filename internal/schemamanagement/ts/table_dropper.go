package ts

import (
	"fmt"
	"github.com/jackc/pgx"
	"log"
)

const (
	dropTableQueryTemplate        = "DROP TABLE %s"
	dropTableCascadeQueryTemplate = "DROP TABLE %s CASCADE"
)

type tableDropper interface {
	Drop(db *pgx.Conn, schema, table string, cascade bool) error
}

type defaultTableDropper struct{}

func newTableDropper() tableDropper {
	return &defaultTableDropper{}
}
func (d *defaultTableDropper) Drop(db *pgx.Conn, schema, table string, cascade bool) error {
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

	log.Printf("Executing: %s", query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
