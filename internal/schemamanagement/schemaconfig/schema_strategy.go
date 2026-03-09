package schemaconfig

import "fmt"

// SchemaStrategy is an enum representing what the ingestor should do
// regarding the schema in the target database
type SchemaStrategy int

// Enum values for SchemaStrategy
const (
	// Validate that the selected database matches the IDRF data set info
	ValidateOnly SchemaStrategy = iota + 1
	// Create the data set info if it's missing, fail if incompatible
	CreateIfMissing
	// Drop existing table and create a new one
	DropAndCreate
	// DROP CASCADE existing table and create a new one
	DropCascadeAndCreate
	// Skip schema validation; adopt the existing table's column types for the transfer
	AdoptExisting
)

func (s SchemaStrategy) String() string {
	switch s {
	case ValidateOnly:
		return "ValidateOnly"
	case CreateIfMissing:
		return "CreateIfMissing"
	case DropCascadeAndCreate:
		return "DropCascadeAndCreate"
	case DropAndCreate:
		return "DropAndCreate"
	case AdoptExisting:
		return "AdoptExisting"
	default:
		panic("unknown type")
	}
}

// ParseStrategyString returns the enum value matching the string, or an error
func ParseStrategyString(strategy string) (SchemaStrategy, error) {
	switch strategy {
	case "ValidateOnly":
		return ValidateOnly, nil
	case "CreateIfMissing":
		return CreateIfMissing, nil
	case "DropCascadeAndCreate":
		return DropCascadeAndCreate, nil
	case "DropAndCreate":
		return DropAndCreate, nil
	case "AdoptExisting":
		return AdoptExisting, nil
	default:
		return ValidateOnly, fmt.Errorf("unknown schema strategy '%s'", strategy)
	}
}
