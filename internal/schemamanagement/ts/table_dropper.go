package ts

import (
	"fmt"
	"log"

	"github.com/jackc/pgx"
)

const (
	dropTableQueryTemplate        = "DROP TABLE %s"
	dropTableCascadeQueryTemplate = "DROP TABLE %s CASCADE"
)

type tableDropper interface {
	Drop(db *pgx.Conn, table string, cascade bool) error
}

type defaultTableDropper struct{}

func newTableDropper() tableDropper {
	return &defaultTableDropper{}
}
func (d *defaultTableDropper) Drop(db *pgx.Conn, table string, cascade bool) error {
	var query string
	if cascade {
		query = fmt.Sprintf(dropTableCascadeQueryTemplate, table)
	} else {
		query = fmt.Sprintf(dropTableQueryTemplate, table)
	}

	log.Printf("Executing: %s", query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
