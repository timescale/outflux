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
	IDRFJson
	IDRFUnknown
)

func (d DataType) String() string {
	switch d {
	case IDRFBoolean:
		return "Boolean"
	case IDRFDouble:
		return "Double"
	case IDRFInteger32:
		return "Integer32"
	case IDRFString:
		return "String"
	case IDRFTimestamp:
		return "IDRFTimestamp"
	case IDRFTimestamptz:
		return "Timestamptz"
	case IDRFInteger64:
		return "Integer64"
	case IDRFSingle:
		return "Single"
	case IDRFJson:
		return "Json"
	case IDRFUnknown:
		return "Unknown"
	default:
		panic("Unexpected value")
	}
}

// CanFitInto returns true if this data type can be safely cast to the other data type
func (d DataType) CanFitInto(other DataType) bool {
	if d == other {
		return true
	}

	switch d {
	case IDRFInteger32:
		return other == IDRFSingle || other == IDRFDouble || other == IDRFInteger64
	case IDRFSingle:
		return other == IDRFDouble
	case IDRFTimestamp:
		return other == IDRFTimestamptz
	}

	return false
}
