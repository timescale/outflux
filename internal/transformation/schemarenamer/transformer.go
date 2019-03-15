package schemarenamer

import (
	"fmt"

	"github.com/timescale/outflux/internal/idrf"
)

// Transformer will hold the implementation for the schema-rename transformer.
type Transformer struct {
	id            string
	outputSchema  string
	prepareCalled bool
}

// NewTransformer returns a new instance of a transformer renames the schema
// of a DataSet ("schema.table"->"schema1.table" or "table"-> "schema1.table")
func NewTransformer(id string, outputSchema string) *Transformer {
	return &Transformer{id: id, outputSchema: outputSchema}
}

// ID returns a string that identifies the transformer instance
func (c *Transformer) ID() string {
	return c.id
}

// Prepare verifies that the transformation can be executed, creates the output channel
// and the transformed data set definition and returns them as a idrf.Bundle
func (c *Transformer) Prepare(input *idrf.Bundle) (*idrf.Bundle, error) {
	originDataSet := input.DataDef
	_, table := originDataSet.SchemaAndTable()
	newDataSetName := idrf.GenerateDataSetIdentifier(c.outputSchema, table)
	newDataSet, err := idrf.NewDataSet(newDataSetName, originDataSet.Columns, originDataSet.TimeColumn)
	if err != nil {
		return nil, fmt.Errorf("%s: could not prepare schema renaming.\n%v", c.id, err)
	}

	// the data doesn't change, only the name of the resulting data set
	// so the data channel is left as is
	c.prepareCalled = true
	return &idrf.Bundle{
		DataDef:  newDataSet,
		DataChan: input.DataChan,
	}, nil
}

// Start doesn't do anything in this transformer since the data is not transformed,
// only the schema (already done in Prepare)
func (c *Transformer) Start(errChan chan error) error {
	if !c.prepareCalled {
		return fmt.Errorf("%s: Prepare must be called before Start", c.id)
	}

	return nil
}
