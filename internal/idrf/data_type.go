package idrf

// DataType Supported data types in the Intermediate Data Representation Format
type DataType int

// Available values for IDRF DataType enum
const (
	IDRFInteger32 DataType = iota + 1
	IDRFInteger64
	IDRFDouble
	IDRFSingle
	IDRFString
	IDRFBoolean
	IDRFTimestamptz
	IDRFTimestamp
	IDRFUnknown
)

func (d DataType) String() string {
	switch d {
	case IDRFBoolean:
		return "IDRFBoolean"
	case IDRFDouble:
		return "IDRFDouble"
	case IDRFInteger32:
		return "IDRFInteger32"
	case IDRFString:
		return "IDRFString"
	case IDRFTimestamp:
		return "IDRFTimestamp"
	case IDRFTimestamptz:
		return "IDRFTimestamptz"
	case IDRFInteger64:
		return "IDRFInteger64"
	case IDRFSingle:
		return "IDRFSingle"
	case IDRFUnknown:
		return "IDRFUnknown"
	default:
		panic("Unexpected value")
	}
}

// CanFitInto returns true if this data type can be safely cast to the other data type
func (d DataType) CanFitInto(other DataType) bool {
	if d == other {
		return true
	}

	if d == IDRFInteger32 {
		return other == IDRFSingle || other == IDRFDouble || other == IDRFInteger64
	}

	if d == IDRFInteger64 || d == IDRFSingle {
		return other == IDRFDouble
	}

	if d == IDRFTimestamp {
		return other == IDRFTimestamptz
	}

	return false
}
