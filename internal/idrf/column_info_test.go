package idrf

import "testing"

func TestNewColumn(t *testing.T) {
	if _, err := NewColumn("", IDRFBoolean); err == nil {
		t.Error("expected error, none received")
	}

	res, err := NewColumn("some name", IDRFBoolean)
	if res == nil || err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	str := res.String()
	if res.Name != "some name" || res.DataType != IDRFBoolean {
		t.Errorf("unexpected values: %s", str)
	}
}
