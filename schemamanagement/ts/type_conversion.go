package ts

import (
	"strings"

	"github.com/timescale/outflux/idrf"
)

func pgTypeToIdrf(pgType string) idrf.DataType {
	lowerCaseType := strings.ToLower(pgType)
	switch lowerCaseType {
	case "text":
		return idrf.IDRFString
	case "timestamp with time zone":
		return idrf.IDRFTimestamptz
	case "timestamp without time zone":
		return idrf.IDRFTimestamp
	case "double precision":
		return idrf.IDRFDouble
	case "integer":
		return idrf.IDRFInteger32
	case "bigint":
		return idrf.IDRFInteger64
	default:
		return idrf.IDRFUnknown
	}
}

func idrfToPgType(dataType idrf.DataType) string {
	switch dataType {
	case idrf.IDRFBoolean:
		return "BOOLEAN"
	case idrf.IDRFDouble:
		return "FLOAT"
	case idrf.IDRFInteger32:
		return "INTEGER"
	case idrf.IDRFString:
		return "TEXT"
	case idrf.IDRFTimestamp:
		return "TIMESTAMP"
	case idrf.IDRFTimestamptz:
		return "TIMESTAMPTZ"
	case idrf.IDRFInteger64:
		return "BIGINT"
	case idrf.IDRFSingle:
		return "FLOAT"
	default:
		panic("Unexpected value")
	}
}
