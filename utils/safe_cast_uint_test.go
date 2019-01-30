package utils

import (
	"math"
	"testing"
)

func TestSafeUInt(t *testing.T) {
	cases := []struct {
		in          uint
		out         int
		errExpected bool
	}{
		{uint(1), int(1), false},
		{uint(math.MaxInt64), math.MaxInt64, false},
		{uint(math.MaxInt64) + uint(1), -1, true},
	}

	for _, tCase := range cases {
		ret, err := SafeCastUInt(tCase.in)
		if tCase.errExpected && err == nil {
			t.Errorf("Expected error, none returned")
		}

		if !tCase.errExpected && err != nil {
			t.Errorf("No error expected, got:%v\n", err)
		}

		if ret != tCase.out {
			t.Errorf("Expected: %d\ngot: %d", tCase.out, ret)
		}
	}
}
