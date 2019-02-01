package ingestion

import (
	"database/sql"
	"testing"

	"github.com/lib/pq"
)

func TestTest(t *testing.T) {
	db, _ := sql.Open("postgres", "postgres://test:test@localhost:5432/test?sslmode=disable")
	transaction, _ := db.Begin()

	copyQuery := pq.CopyIn("a", "a")
	statement, _ := transaction.Prepare(copyQuery)

	for i := 1; i < 10; i++ {
		statement.Exec(i)
		transaction.Commit()
	}

	statement.Close()
	transaction.Commit()
}
