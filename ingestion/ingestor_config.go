package ingestion

// IngestorConfig holds all the properties required to create and run an ingestor
type IngestorConfig struct {
	IngestorID              string
	BatchSize               uint16
	RollbackOnExternalError bool
}
