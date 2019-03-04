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
				t.Errorf("should've fit")
			}
		} else if dt != IDRFInteger32 && IDRFInteger32.CanFitInto(dt) {
			t.Error("shouldn't have fit")
			continue
		}

		if dt == IDRFDouble {
			if !IDRFInteger64.CanFitInto(dt) && !IDRFSingle.CanFitInto(dt) {
				t.Error("should've fit")
			}
		} else if dt != IDRFInteger64 && dt != IDRFSingle && (IDRFInteger64.CanFitInto(dt) || IDRFSingle.CanFitInto(dt)) {
			t.Error("shouldn't have")
			continue
		}

		if dt == IDRFTimestamptz && !IDRFTimestamp.CanFitInto(dt) {
			t.Error("should've fit")
			continue
		}

		if dt != IDRFString && dt.CanFitInto(IDRFString) {
			t.Error("shoulnd't have fit")
		}
	}
}
