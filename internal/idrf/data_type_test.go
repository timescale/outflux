package idrf

import "testing"

func TestCanFitInto(t *testing.T) {
	allTypes := []DataType{
		IDRFInteger32,
		IDRFInteger64,
		IDRFDouble,
		IDRFSingle,
		IDRFString,
		IDRFBoolean,
		IDRFTimestamptz,
		IDRFTimestamp,
		IDRFJson,
		IDRFUnknown,
	}

	for _, dt := range allTypes {
		if !dt.CanFitInto(dt) {
			t.Errorf("%v can't fit into himself", dt)
		}
	}

	for _, dt := range allTypes {
		if dt == IDRFInteger64 || dt == IDRFSingle || dt == IDRFDouble {
			if !IDRFInteger32.CanFitInto(dt) {
				t.Errorf("%s should've fit in %s", IDRFInteger32, dt)
			}
		} else if dt != IDRFInteger32 && IDRFInteger32.CanFitInto(dt) {
			t.Errorf("%s shouldn't have fit in %s", IDRFInteger32, dt)
			continue
		}

		if dt == IDRFDouble {
			if !IDRFSingle.CanFitInto(dt) {
				t.Errorf("%s should've fit in %s", IDRFSingle, dt)
			}
		} else if dt != IDRFSingle && IDRFSingle.CanFitInto(dt) {
			t.Errorf("%s shouldn't have fit in %s", IDRFSingle, dt)
			continue
		}

		if dt == IDRFTimestamptz && !IDRFTimestamp.CanFitInto(dt) {
			t.Errorf("%s should've fit in %s", IDRFTimestamp, dt)
			continue
		}

		if dt != IDRFString && dt.CanFitInto(IDRFString) {
			t.Errorf("%s shouldn't have fit in %s", dt, IDRFString)
		}
	}
}
