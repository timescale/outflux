package idrf

// Bundle defines a bundle of a data definition (schema) and a channel that caries data in IDRF format
type Bundle struct {
	DataDef  *DataSet
	DataChan chan Row
}
