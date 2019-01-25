package config

// Config holds all the properties required to create and run an ingestor
type Config struct {
	BatchSize            uint
	Server               string
	Username             string
	Password             string
	SchemaStrategy       SchemaStrategy
	Database             string
	AdditionalConnParams map[string]string
	Schema               string
}

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
	default:
		panic("unknown type")
	}
}
