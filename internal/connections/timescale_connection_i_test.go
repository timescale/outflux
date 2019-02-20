// +build integration

package connections

import (
	"os"
	"testing"

	"github.com/timescale/outflux/internal/testutils"
)

func TestNewConnection(t *testing.T) {
	db := "test_new_conn"
	testutils.CreateTimescaleDb(t, db)
	defer testutils.DeleteTimescaleDb(t, db)

	goodEnv := map[string]string{
		"PGPORT":     "5433",
		"PGUSER":     "postgres",
		"PGPASSWORD": "postgres",
		"PGDATABASE": db,
	}

	badEnv := map[string]string{
		"PGPORT":     "5433",
		"PGUSER":     "postgres",
		"PGPASSWORD": "postgres",
		"PGDATABASE": "wrong_db",
	}
	connService := &defaultTSConnectionService{}
	testCases := []struct {
		desc      string
		conn      string
		env       map[string]string
		expectErr bool
	}{
		{desc: "nothing is set, env is empty", expectErr: true},
		{desc: "enviroment is set, no overrides", env: goodEnv},
		{desc: "enviroment is set, overrides make is bad", env: goodEnv, conn: "dbname=wrong_db", expectErr: true},
		{desc: "enviroment is set badly, overrides make it good", env: badEnv, conn: "dbname=" + db},
	}

	for _, tc := range testCases {
		// make sure the environment is only that in tc.env
		os.Clearenv()
		for k, v := range tc.env {
			os.Setenv(k, v)
		}
		res, err := connService.NewConnection(tc.conn)
		if err != nil && !tc.expectErr {
			t.Errorf("%s\nunexpected error: %v", tc.desc, err)
		} else if err == nil && tc.expectErr {
			res.Close()
			t.Errorf("%s\nexpected error, none received", tc.desc)
		}

		if tc.expectErr {
			continue
		}

		rows, err := res.Query("SELECT 1")
		if err != nil {
			t.Error("could execute query with established connection")
			continue
		}

		if !rows.Next() {
			t.Error("no result returned for SELECT 1")
		} else {
			var dest int
			rows.Scan(&dest)
			if dest != 1 {
				t.Errorf("expected 1, got %d", dest)
			}
		}

		rows.Close()
		res.Close()
	}
}
