package ts

import (
	"testing"

	"github.com/timescale/outflux/internal/idrf"
)

func TestPgTypeToIdrf(t *testing.T) {
	testCases := []struct {
		in  string
		out idrf.DataType
	}{
		{"text", idrf.IDRFString},
		{"timestamp with time zone", idrf.IDRFTimestamptz},
		{"timestamp without time zone", idrf.IDRFTimestamp},
		{"double precision", idrf.IDRFDouble},
		{"integer", idrf.IDRFInteger32},
		{"bigint", idrf.IDRFInteger64},
		{"asdasd", idrf.IDRFUnknown},
	}

	for _, tc := range testCases {
		res := pgTypeToIdrf(tc.in)
		if res != tc.out {
			t.Errorf("Expected %v, got %v", tc.out, res)
		}
	}
}
