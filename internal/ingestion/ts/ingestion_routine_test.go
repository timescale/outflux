package ts

import (
	"errors"
	"testing"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"
	"github.com/timescale/outflux/internal/connections"
)

func TestOpenTx(t *testing.T) {
	res, err := openTx(&ingestDataArgs{
		dbConn: &connections.MockPgxW{
			BeginRes: []*pgx.Tx{nil},
			BeginErr: []error{errors.New("generic error")},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, res)
	res, err = openTx(&ingestDataArgs{
		dbConn: &connections.MockPgxW{
			BeginRes: []*pgx.Tx{&pgx.Tx{}},
			BeginErr: []error{nil},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func TestCopyToDb(t *testing.T) {
	assert.Panics(t, func() {
		copyToDb(&ingestDataArgs{
			dbConn: &connections.MockPgxW{
				CopyFromErr: []error{errors.New("err")},
			},
		}, &pgx.Identifier{"x"}, &pgx.Tx{}, [][]interface{}{})
	}, "should panic because of tx.Rollback")
	mock := &connections.MockPgxW{CopyFromErr: []error{nil}}
	copyToDb(&ingestDataArgs{
		dbConn:   mock,
		colNames: []string{"a"},
	}, &pgx.Identifier{"x"}, &pgx.Tx{}, [][]interface{}{})
	assert.Equal(t, mock.ExpCopyFromTab[0], pgx.Identifier{"x"})
	assert.Equal(t, mock.ExpCopyFromCol, [][]string{[]string{"a"}})
}
