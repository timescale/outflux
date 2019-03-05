package ts

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestIdrfToPgType(t *testing.T) {
	testCases := []struct {
		in  idrf.DataType
		out string
	}{
		{idrf.IDRFBoolean, "BOOLEAN"},
		{idrf.IDRFDouble, "FLOAT"},
		{idrf.IDRFInteger32, "INTEGER"},
		{idrf.IDRFInteger64, "BIGINT"},
		{idrf.IDRFString, "TEXT"},
		{idrf.IDRFTimestamp, "TIMESTAMP"},
		{idrf.IDRFTimestamptz, "TIMESTAMPTZ"},
		{idrf.IDRFSingle, "FLOAT"},
		{idrf.IDRFJson, "JSONB"},
	}

	for _, tc := range testCases {
		res := idrfToPgType(tc.in)
		if res != tc.out {
			t.Errorf("Expected:%v\ngot:%v", tc.out, res)
		}
	}
}
